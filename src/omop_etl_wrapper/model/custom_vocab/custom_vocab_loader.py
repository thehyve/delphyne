import logging
from pathlib import Path
from typing import List

from .base_manager import BaseVocabManager, BaseClassManager, BaseConceptManager
from ..._paths import CUSTOM_VOCAB_DIR
from ...database import Database
from ...util.io import get_all_files_in_dir, valid_or_null_prefix

logger = logging.getLogger(__name__)


class CustomVocabLoader(BaseVocabManager, BaseClassManager, BaseConceptManager):
    def __init__(self, db: Database, cdm):
        self.db = db
        self._cdm = cdm
        self._custom_vocab_files = self._get_custom_table_files('vocabulary')
        self._custom_class_files = self._get_custom_table_files('concept_class')
        self._custom_concept_files = self._get_custom_table_files('concept')

        BaseVocabManager.__init__(self, db=self.db, cdm=self._cdm,
                                  custom_vocab_files=self._custom_vocab_files)
        BaseClassManager.__init__(self, db=self.db, cdm=self._cdm,
                                  custom_class_files=self._custom_class_files)
        BaseConceptManager.__init__(self, db=self.db, cdm=self._cdm,
                                    custom_concept_files=self._custom_concept_files)

    @staticmethod
    def _get_custom_table_files(omop_table: str) -> List[Path]:
        # Get custom vocab files for a specific vocabulary target table
        # based on the file name conventions (e.g. "concept").
        custom_table_files = get_all_files_in_dir(CUSTOM_VOCAB_DIR)
        return [f for f in custom_table_files if f.stem.endswith(omop_table)]

    def _update_custom_files(self, file_list: List[Path], omop_table: str) -> List[Path]:
        # Check if file has either a valid suffix (matching a
        # vocabulary_id to be updated) or no suffix; a mismatching
        # suffix will cause the file to be ignored.
        return [f for f in file_list
                if valid_or_null_prefix(f, omop_table, self._custom_vocabs_to_update)]

    def load_custom_vocabulary_tables(self) -> None:
        # check vocabs and classes to drop and update
        self._get_custom_vocabulary_sets()
        self._get_custom_class_sets()
        # get vocabularies ids for Concept table operations
        vocabs_to_load = self.vocabs_updated
        vocabs_to_drop = self.vocabs_updated | self.vocabs_unused

        # update list of concept files to parse to only include those
        # without prefix, or prefix matching vocab ids to update
        self._custom_concept_files = self._update_custom_files(
            self._custom_concept_files, 'concept')

        if vocabs_to_load and not self._custom_concept_files:
            raise Exception('Found vocabs to load but no valid custom concept files, '
                            'check for mismatches between new vocab ids and concept files prefix')

        # drop old versions (unused + updated)
        self._drop_custom_concepts(vocabs_to_drop)
        self._drop_custom_vocabs()
        self._drop_custom_classes()
        # load new versions (update in place)
        self._update_custom_classes()
        # load new versions (create new records)
        self._load_custom_classes()
        self._load_custom_vocabs()
        self._load_custom_concepts(vocabs_to_load)
