"""concept vocabulary table operations."""

import csv
import logging
from pathlib import Path
from typing import Set, List

from ....database import Database
from ....util.io import get_file_prefix

logger = logging.getLogger(__name__)


class ConceptManager:
    """
    Collection of concept vocabulary table functions.

    Parameters
    ----------
    db : Database
        Database instance to interact with.
    cdm : module
        Module containing all CDM table definitions.
    custom_concept_files : list of pathlib.Path
        Collection of files containing custom concept data.
    """

    def __init__(self, db: Database, cdm, custom_concept_files: List[Path]):
        self._db = db
        self._cdm = cdm
        self.custom_concept_files = custom_concept_files

        if not self.custom_concept_files:
            logger.error('No concept.tsv file found')

    def drop_custom_concepts(self, vocab_ids: Set[str]) -> None:
        """
        Drop obsolete custom concept records from the database.

        Parameters
        ----------
        vocab_ids: set of str
            Only concepts associated with the provided set of
            vocabulary_ids will be dropped.

        """
        logger.info(f'Dropping old custom concepts: '
                    f'{True if vocab_ids else False}')

        if not vocab_ids:
            return

        with self._db.tracked_session_scope(name='drop_concepts') as (session, _):
            session.query(self._cdm.Concept) \
                .filter(self._cdm.Concept.vocabulary_id.in_(vocab_ids)) \
                .delete(synchronize_session=False)

    def load_custom_concepts(self, vocab_ids: Set[str], valid_prefixes: Set[str]) -> None:
        """
        Load new custom concept records to the database.

        Parameters
        ----------
        vocab_ids : set of str
            Set of new or updated vocabulary_ids. Only concepts
            associated with these vocabulary_ids will be loaded.
        valid_prefixes : set of str
            Set of all user-provided vocabulary ids. Allows detection
            of concept_ids with an unknown vocabulary_id or a valid
            vocabulary_id that doesn't match the file prefix.

        """
        logger.info(f'Loading new custom concept_ids: '
                    f'{True if vocab_ids else False}')

        if not vocab_ids:
            return

        unique_concepts_check = set()
        vocabs_lowercase = {vocab.lower() for vocab in valid_prefixes}

        errors = set()
        files_with_errors = set()

        for concept_file in self.custom_concept_files:

            file_errors = False

            file_prefix = get_file_prefix(concept_file, 'concept')
            check_prefix = file_prefix in vocabs_lowercase
            invalid_vocabs = set()
            unknown_vocabs = set()

            with self._db.tracked_session_scope(name=f'load_{concept_file.stem}') \
                    as (session, _), concept_file.open('r') as f_in:

                rows = csv.DictReader(f_in, delimiter='\t')

                for row in rows:
                    concept_id = row['concept_id']
                    vocabulary_id = row['vocabulary_id']

                    # skip concept_ids with unknown vocabulary_id.
                    if vocabulary_id not in valid_prefixes:
                        unknown_vocabs.add(vocabulary_id)
                        continue

                    # if file prefix is valid vocab_id,
                    # vocabulary_ids in file should match it.
                    # the concept_id will be loaded irrespectively.
                    # comparison is case-insensitive.
                    if check_prefix and vocabulary_id.lower() != file_prefix:
                        invalid_vocabs.add(vocabulary_id)

                    # quality checks
                    # NOTE: only performing checks not already enforced
                    # by CONCEPT table constraints; those will raise an
                    # exception as soon as they occur
                    if int(concept_id) < 2_000_000_000:
                        errors.add(f'{concept_file.name} must contain concept_ids starting at '
                                   f'2\'000\'000\'000 (2B+ convention)')
                        file_errors = True
                        continue
                    if concept_id in unique_concepts_check:
                        errors.add(f'concept {concept_id} is duplicated across one or multiple '
                                   f'files')
                        file_errors = True
                        continue

                    unique_concepts_check.add(concept_id)

                    if vocabulary_id in vocab_ids:
                        session.add(self._cdm.Concept(
                            concept_id=row['concept_id'],
                            concept_name=row['concept_name'],
                            domain_id=row['domain_id'],
                            vocabulary_id=row['vocabulary_id'],
                            concept_class_id=row['concept_class_id'],
                            standard_concept=row['standard_concept'],
                            concept_code=row['concept_code'],
                            valid_start_date=row['valid_start_date'],
                            valid_end_date=row['valid_end_date'],
                            invalid_reason=row['invalid_reason']
                        ))

            if file_errors:
                files_with_errors.add(concept_file.name)

            if unknown_vocabs:
                logger.warning(f'{concept_file.name} contains unknown vocabulary_ids: '
                               f'{unknown_vocabs}')
            if invalid_vocabs:
                logger.warning(f'{concept_file.name} contains valid vocabulary_ids '
                               f'that do not match file prefix: {invalid_vocabs}')

        if files_with_errors:
            for error in sorted(errors):
                logger.error(error)
            files_with_errors = sorted(files_with_errors)
            raise ValueError(f'Concept files {files_with_errors} contain invalid values')
