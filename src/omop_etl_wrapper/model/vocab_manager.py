from pathlib import Path
from typing import List

from .._paths import CUSTOM_VOCAB_DIR
from ..database import Database
from ..util.io import is_hidden
from .base_manager import BaseVocabManager, BaseClassManager, BaseConceptManager
import logging


logger = logging.getLogger(__name__)


class VocabularyLoader(BaseVocabManager, BaseClassManager, BaseConceptManager):
    def __init__(self, db: Database, cdm):
        self.db = db
        self._cdm = cdm
        self._custom_vocab_files = self._subset_custom_table_files('vocabulary')
        self._custom_concept_files = self._subset_custom_table_files('concept')
        self._custom_class_files = self._subset_custom_table_files('concept_class')

        BaseVocabManager.__init__(self, db=self.db, cdm=self._cdm,
                                  custom_vocab_files=self._custom_vocab_files)
        BaseClassManager.__init__(self, db=self.db, cdm=self._cdm,
                                  custom_class_files=self._custom_class_files)
        BaseConceptManager.__init__(self, db=self.db, cdm=self._cdm,
                                    custom_concept_files=self._custom_concept_files)

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
