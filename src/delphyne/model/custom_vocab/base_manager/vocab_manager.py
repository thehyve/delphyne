"""vocabulary table operations."""

import csv
import logging
from functools import lru_cache
from pathlib import Path
from typing import List, Dict

from ....database import Database
from ....util.io import get_file_prefix

logger = logging.getLogger(__name__)


class VocabManager:
    """
    Collection of vocabulary table functions.

    Parameters
    ----------
    db : Database
        Database instance to interact with.
    cdm : module
        Module containing all CDM table definitions.
    custom_vocab_files : list of pathlib.Path
        Collection of files containing custom vocabulary data.
    """

    def __init__(self, db: Database, cdm, custom_vocab_files: List[Path]):
        self._db = db
        self._cdm = cdm
        self.custom_vocab_files = custom_vocab_files
        self.custom_vocabs_to_update = set()
        self.custom_vocabs_unused = set()

        if not self.custom_vocab_files:
            logger.error('No vocabulary.tsv file found')

    @property
    def vocabs_updated(self):
        """Set of vocabulary IDs to update."""
        return self.custom_vocabs_to_update

    @property
    def vocabs_unused(self):
        """Vocabulary IDs of no longer used vocabularies."""
        return self.custom_vocabs_unused

    @property
    @lru_cache()
    def vocabs_from_disk(self) -> Dict[str, str]:
        """User-provided custom vocabulary IDs and versions."""
        vocab_dict = {}
        errors = set()
        files_with_errors = set()

        for vocab_file in self.custom_vocab_files:

            file_errors = False

            with open(vocab_file) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    vocab_id = row['vocabulary_id']
                    version = row['vocabulary_version']
                    reference = row['vocabulary_reference']
                    concept_id = row['vocabulary_concept_id']

                    # quality checks
                    if not vocab_id:
                        errors.add(f'{vocab_file.name} may not contain an empty vocabulary_id')
                        file_errors = True
                    if not version:
                        errors.add(f'{vocab_file.name} may not contain an empty'
                                   f' vocabulary_version')
                        file_errors = True
                    if not reference:
                        errors.add(f'{vocab_file.name} may not contain an empty'
                                   f' vocabulary_reference')
                        file_errors = True
                    if concept_id != '0':
                        errors.add(f'{vocab_file.name} may not contain vocabulary_concept_id'
                                   f' other than 0')
                        file_errors = True
                    if vocab_id in vocab_dict:
                        errors.add(f'vocabulary {vocab_id} is duplicated across one or multiple'
                                   f' files')
                        file_errors = True

                    vocab_dict[vocab_id] = version

            if file_errors:
                files_with_errors.add(vocab_file.name)

        if files_with_errors:
            for error in sorted(errors):
                logger.error(error)
            files_with_errors = sorted(files_with_errors)
            raise ValueError(f'Vocabulary files {files_with_errors} contain invalid values')

        return vocab_dict

    @property
    @lru_cache()
    def vocabs_from_database(self) -> Dict[str, str]:
        """Get custom vocabularies (vocabulary_concept_id == 0)."""
        vocab_dict = {}

        with self._db.session_scope() as session:
            records = session.query(self._cdm.Vocabulary) \
                .filter(self._cdm.Vocabulary.vocabulary_concept_id == 0) \
                .all()

            for record in records:
                vocab_dict[record.vocabulary_id] = record.vocabulary_version

        return vocab_dict

    def get_custom_vocabulary_sets(self) -> None:
        """
        Compare custom vocabularies between files and database.

        Only detects differences in vocabulary_version.

        Retrieves:
        - a set of custom vocabularies to be updated
          (new id, or same id and new version)
        - a set of obsolete custom vocabularies to be removed
          (id not in use anymore)

        """
        logger.info('Looking for new custom vocabulary versions')

        vocab_old = self.vocabs_from_database
        vocab_new = self.vocabs_from_disk

        unchanged_vocabs = set()

        for new_id, new_version in vocab_new.items():
            old_version = vocab_old.get(new_id, None)

            # skip version if already present in database
            if old_version == new_version:
                unchanged_vocabs.add(new_id)
            else:
                # if vocabulary didn't exist before, old_version is None
                logger.info(f'Found new vocabulary version: {new_id} : '
                            f'{old_version} -> {new_version}')

        self.custom_vocabs_to_update = vocab_new.keys() - unchanged_vocabs
        if not self.custom_vocabs_to_update:
            logger.info('No new vocabulary version found on disk')

        logger.info('Looking for unused custom vocabulary versions')

        self.custom_vocabs_unused = vocab_old.keys() - vocab_new.keys()

        for old_id in self.custom_vocabs_unused:
            logger.info(f'Found obsolete vocabulary version: {old_id}')

        if not self.custom_vocabs_unused:
            logger.info('No obsolete version found in database')

    def drop_custom_vocabs(self) -> None:
        """Drop obsolete custom vocabularies from the database."""
        vocabs_to_drop = self.custom_vocabs_to_update | self.custom_vocabs_unused

        logger.info(f'Dropping old custom vocabulary versions: '
                    f'{True if vocabs_to_drop else False}')

        if not vocabs_to_drop:
            return

        with self._db.tracked_session_scope(name='drop_vocabs') as (session, _):
            session.query(self._cdm.Vocabulary) \
                .filter(self._cdm.Vocabulary.vocabulary_id.in_(vocabs_to_drop)) \
                .delete(synchronize_session=False)

    def load_custom_vocabs(self) -> None:
        """
        Load custom vocabulary records to the database.

        Both new and updated vocabularies will be loaded.
        """
        vocabs_to_create = self.custom_vocabs_to_update

        logger.info(f'Loading new custom vocabulary versions: '
                    f'{True if vocabs_to_create else False}')

        if not vocabs_to_create:
            return

        ignored_vocabs = set()
        vocabs_lowercase = {vocab.lower() for vocab in self.vocabs_from_disk}

        for vocab_file in self.custom_vocab_files:

            file_prefix = get_file_prefix(vocab_file, 'vocabulary')
            invalid_vocabs = set()

            with self._db.tracked_session_scope(name=f'load_{vocab_file.stem}') \
                    as (session, _), vocab_file.open('r') as f_in:
                rows = csv.DictReader(f_in, delimiter='\t')

                for row in rows:
                    vocabulary_id = row['vocabulary_id']

                    if vocabulary_id in vocabs_to_create:
                        session.add(self._cdm.Vocabulary(
                            vocabulary_id=row['vocabulary_id'],
                            vocabulary_name=row['vocabulary_name'],
                            vocabulary_reference=row['vocabulary_reference'],
                            vocabulary_version=row['vocabulary_version'],
                            vocabulary_concept_id=row['vocabulary_concept_id']
                        ))
                    else:
                        ignored_vocabs.add(vocabulary_id)

                    # if file prefix is valid vocab_id,
                    # vocabulary_ids in file should match it.
                    # comparison is case-insensitive.
                    if file_prefix in vocabs_lowercase and vocabulary_id.lower() != file_prefix:
                        invalid_vocabs.add(vocabulary_id)

            if invalid_vocabs:
                logger.warning(f'{vocab_file.name} contains vocabulary_ids '
                               f'that do not match file prefix: {invalid_vocabs}')

        if ignored_vocabs:
            logger.debug(f'Skipped records with vocabulary_id values that '
                         f'were already loaded under the current version: '
                         f'{ignored_vocabs}')
