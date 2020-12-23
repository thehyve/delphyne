import csv
import logging
from pathlib import Path
from typing import List, Dict

from ....database import Database

logger = logging.getLogger(__name__)


class BaseVocabManager:
    """
    Collects functions that interact with the Vocabulary table.
    """

    def __init__(self, db: Database, cdm, custom_vocab_files: List[Path]):
        self.db = db
        self._cdm = cdm
        self._custom_vocab_files = custom_vocab_files
        self._custom_vocabs_to_update = set()
        self._custom_vocabs_unused = set()

    @property
    def vocabs_updated(self):
        return self._custom_vocabs_to_update

    @property
    def vocabs_unused(self):
        return self._custom_vocabs_unused

    def _get_custom_vocabulary_sets(self) -> None:
        # Compare custom vocabulary ids from disk
        # to the ones already present in the database.
        #
        # Retrieves:
        # - a set of custom vocabularies to be updated
        #   (new id, or same id and new version)
        # - a set of obsolete custom vocabularies to be removed
        #   (id is not in use anymore)

        logging.info('Looking for new custom vocabulary versions')

        vocab_old = self._get_old_custom_vocabs_from_database()
        vocab_new = self._get_new_custom_vocabs_from_disk()

        unchanged_vocabs = set()

        for new_id, new_version in vocab_new.items():
            old_version = vocab_old.get(new_id, None)

            # skip version if already present in database
            if old_version == new_version:
                unchanged_vocabs.add(new_id)
                continue
            else:
                # if vocabulary didn't exist before, old_version is None
                logging.info(f'Found new vocabulary version: {new_id} : '
                             f'{old_version} ->  {new_version}')

        self._custom_vocabs_to_update = set(vocab_new.keys()) - unchanged_vocabs
        if not self._custom_vocabs_to_update:
            logging.info('No new vocabulary version found on disk')

        logging.info('Looking for unused custom vocabulary versions')

        self._custom_vocabs_unused = \
            set(vocab_old.keys()) - set(vocab_new.keys())

        for old_id in self._custom_vocabs_unused:
            logging.info(f'Found obsolete vocabulary version: {old_id}')

        if not self._custom_vocabs_unused:
            logging.info('No obsolete version found in database')

    def _get_old_custom_vocabs_from_database(self) -> Dict[str, str]:
        # Retrieve all custom vocabularies (vocabulary_concept_id == 0)
        # currently in database and return a dictionary {id : version}.

        vocab_dict = {}

        with self.db.session_scope() as session:

            records = session.query(self._cdm.Vocabulary) \
                .filter(self._cdm.Vocabulary.vocabulary_concept_id == 0) \
                .all()

            for record in records:
                vocab_dict[record.vocabulary_id] = record.vocabulary_version

        return vocab_dict

    def _get_new_custom_vocabs_from_disk(self) -> Dict[str, str]:
        # Retrieve all user-provided custom vocabularies from disk
        # and return a dictionary {id : version}.

        vocab_dict = {}

        for vocab_file in self._custom_vocab_files:

            with open(vocab_file) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    vocab_id = row['vocabulary_id']
                    version = row['vocabulary_version']
                    reference = row['vocabulary_reference']
                    concept_id = row['vocabulary_concept_id']

                    # quality checks
                    if not vocab_id:
                        raise ValueError(f'{vocab_file.name} may not contain an empty '
                                         f'vocabulary_id')
                    if not version:
                        raise ValueError(f'{vocab_file.name} may not contain an empty '
                                         f'vocabulary_version')
                    if not reference:
                        raise ValueError(f'{vocab_file.name} may not contain an empty '
                                         f'vocabulary_reference')
                    if concept_id != '0':
                        raise ValueError(f'{vocab_file.name} must have vocabulary_concept_id '
                                         f'set to 0')
                    if vocab_id in vocab_dict.keys():
                        raise ValueError(f'{vocab_id} has duplicates across one or multiple files')

                    vocab_dict[vocab_id] = version

        return vocab_dict

    def _drop_custom_vocabs(self) -> None:
        # Drop updated and obsolete custom vocabularies
        # from the database

        vocabs_to_drop = self._custom_vocabs_to_update | self._custom_vocabs_unused

        logging.info(f'Dropping old custom vocabulary versions: '
                     f'{True if vocabs_to_drop else False}')

        if vocabs_to_drop:
            with self.db.session_scope() as session:
                session.query(self._cdm.Vocabulary) \
                    .filter(self._cdm.Vocabulary.vocabulary_id.in_(vocabs_to_drop)) \
                    .delete(synchronize_session=False)

    def _load_custom_vocabs(self) -> None:
        # Load new and updated custom vocabularies to the database

        vocabs_to_create = self._custom_vocabs_to_update

        logging.info(f'Loading new custom vocabulary versions: '
                     f'{True if vocabs_to_create else False}')

        if vocabs_to_create:

            with self.db.session_scope() as session:

                records = []

                for vocab_file in self._custom_vocab_files:
                    with open(vocab_file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        for row in reader:
                            if row['vocabulary_id'] in vocabs_to_create:
                                records.append(self._cdm.Vocabulary(
                                    vocabulary_id=row['vocabulary_id'],
                                    vocabulary_name=row['vocabulary_name'],
                                    vocabulary_reference=row['vocabulary_reference'],
                                    vocabulary_version=row['vocabulary_version'],
                                    vocabulary_concept_id=row['vocabulary_concept_id']
                                ))
                session.add_all(records)
