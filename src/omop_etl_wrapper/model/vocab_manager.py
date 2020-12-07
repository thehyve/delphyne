from pathlib import Path
from typing import List, Union
import csv

from .._paths import CUSTOM_VOCAB_DIR
from ..database import Database
from ..util.io import is_hidden
from .table_manager import VocabManager, ClassManager
import logging


logger = logging.getLogger(__name__)


class VocabularyLoader(VocabManager, ClassManager):
    def __init__(self, db: Database, cdm):
        self.db = db
        self._cdm = cdm
        self._custom_vocab_files = self._subset_custom_table_files('vocabulary')
        self._custom_concept_files = self._subset_custom_table_files('concept')
        self._custom_class_files = self._subset_custom_table_files('concept_class')

        VocabManager.__init__(self, db=self.db, cdm=self._cdm,
                              custom_vocab_files=self._custom_vocab_files)
        ClassManager.__init__(self, db=self.db, cdm=self._cdm,
                              custom_class_files=self._custom_class_files)

    @staticmethod
    def _get_all_custom_table_files() -> List[Path]:
        return [f for f in CUSTOM_VOCAB_DIR.glob('*') if f.is_file()
                and not is_hidden(f)]

    def _subset_custom_table_files(self, omop_table: str) -> List[Path]:
        # get custom vocab files for a specific vocabulary target table
        # based on the file name conventions (e.g. "concept")
        custom_table_files = self._get_all_custom_table_files()
        return [f for f in custom_table_files if f.stem.endswith(omop_table)]

    def load_custom_vocabulary_tables(self) -> None:
        """
        Loads custom vocabularies to the vocabulary schema.
        More in detail:
        1. Checks for the presence of custom vocabularies and
        concept_classes at a predefined folder location;
        2. Compares the version of custom vocabularies and
        concept_classes in the folder to that of custom vocabularies
        and tables already present in the database;
        3. Deletes obsolete versions from the database;
        4. Loads the new versions to the database.
        :return: None
        """

        # get vocabularies and classes that need to be updated
        vocab_ids = self._get_new_custom_vocabulary_ids()
        class_ids = self._get_new_custom_concept_class_ids()

        # drop older version
        self._drop_custom_concepts(vocab_ids)
        self._drop_custom_vocabularies(vocab_ids)
        self._drop_custom_classes(class_ids)
        # load new version
        self._load_custom_classes(class_ids)
        self._load_custom_vocabularies(vocab_ids)
        self._load_custom_concepts(vocab_ids)

    def _drop_custom_concepts(self, vocab_ids: List[str]) -> None:
        # Drop concepts associated with a list of custom vocabulary ids from the database

        logging.info(f'Dropping old custom concepts: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.Concept) \
                    .filter(self._cdm.Concept.vocabulary_id.in_(vocab_ids)) \
                    .delete(synchronize_session=False)

    def _load_custom_concepts(self, vocab_ids: List[str]) -> None:
        # Load concept_ids associated with a list of custom vocabulary ids to the database

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
