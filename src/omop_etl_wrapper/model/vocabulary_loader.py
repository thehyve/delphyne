from pathlib import Path
from typing import List

from .._paths import CUSTOM_VOCAB_DIR
from ..database import Database
from ..util.io import is_hidden


class VocabularyLoader:
    def __init__(self, db: Database, cdm):
        self.db = db

        self._cdm = cdm
        self._custom_vocab_files = self._get_all_custom_vocab_files()
        print(self._get_loaded_vocab_versions())

    @staticmethod
    def _get_all_custom_vocab_files() -> List[Path]:
        return [f for f in CUSTOM_VOCAB_DIR.glob('*') if f.is_file()
                and not is_hidden(f)]

    def _subset_custom_vocab_files(self, omop_table: str) -> List[Path]:
        # get custom vocab files to be loaded into the vocabulary table
        return [f for f in self._custom_vocab_files if f.stem.endswith(omop_table)]

    def _get_loaded_vocab_versions(self):
        with self.db.session_scope() as session:
            vocabs = session.query(self._cdm.Vocabulary).all()
            return {v.vocabulary_id: v.vocabulary_version for v in vocabs}
