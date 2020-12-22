import csv
import logging
from pathlib import Path
from typing import List

from ....database import Database

logger = logging.getLogger(__name__)


class BaseConceptManager:
    """
    Collects functions that interact with the Concept table.
    """

    def __init__(self, db: Database, cdm, custom_concept_files: List[Path]):
        self.db = db
        self._cdm = cdm
        self._custom_concept_files = custom_concept_files

    def _drop_custom_concepts(self, vocab_ids: List[str]) -> None:
        # Drop concepts associated with a list of custom vocabulary ids
        # from the database

        logging.info(f'Dropping old custom concepts: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.Concept) \
                    .filter(self._cdm.Concept.vocabulary_id.in_(vocab_ids)) \
                    .delete(synchronize_session=False)

    def _load_custom_concepts(self, vocab_ids: List[str]) -> None:
        # Load concept_ids associated with a list of custom
        # vocabulary ids to the database

        logging.info(f'Loading new custom concept_ids: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:

            with self.db.session_scope() as session:

                records = []

                for concept_file in self._custom_concept_files:

                    with open(concept_file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        for row in reader:
                            if row['vocabulary_id'] in vocab_ids:
                                records.append(self._cdm.Concept(
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
                    session.add_all(records)
