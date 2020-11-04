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

        # Test for getting versions of loaded vocabularies
        # print(self._get_loaded_vocab_versions())

    @staticmethod
    def _get_all_custom_vocab_files() -> List[Path]:
        return [f for f in CUSTOM_VOCAB_DIR.glob('*') if f.is_file()
                and not is_hidden(f)]

    def _subset_custom_vocab_files(self, omop_table: str) -> List[Path]:
        # get custom vocab files for a specific vocabulary target table
        # based on the file name conventions (e.g. "concept")
        return [f for f in self._custom_vocab_files if f.stem.endswith(omop_table)]

    def _get_loaded_vocab_versions(self):
        with self.db.session_scope() as session:
            vocabs = session.query(self._cdm.Vocabulary).all()
            return {v.vocabulary_id: v.vocabulary_version for v in vocabs}

    def load_custom_vocabulary_tables(self):

        # patterns
        VOCAB_FILE_PATTERN = '*_vocabulary.tsv'
        CLASS_FILE_PATTERN = '*_concept_class.tsv'
        CONCEPT_FILE_PATTERN = '*_concept.tsv'

        # TODO: quality checks: mandatory fields, dependencies
        # self.check_custom_vocabularies_format()

        vocab_ids, vocab_files = self.get_custom_vocabulary_ids_and_files(VOCAB_FILE_PATTERN)
        class_ids, class_files = self.get_custom_class_ids_and_files(CLASS_FILE_PATTERN)

        # drop older versions
        self.drop_custom_concepts(vocab_ids)
        self.drop_custom_vocabularies(vocab_ids)
        self.drop_custom_classes(class_ids)
        # load new versions
        self.load_custom_classes(class_ids, class_files)
        self.load_custom_vocabularies(vocab_ids, vocab_files)
        self.load_custom_concepts(vocab_ids, CONCEPT_FILE_PATTERN)
        # TODO: remove obsolete versions (i.e. cleanup in case of renaming of vocabs/classes);
        #  if the name has been changed, the previous drop won't find them;
        #  NOTE: for this to work, you need to keep a list of valid Athena vocabulary ids
        #  and check that no unknown vocabulary is present (not in Athena or custom vocab files);
        #  the cleanup could be rather time-consuming and should not be executed every time
        valid_vocabs = self.get_list_of_valid_vocabularies()
        self.drop_unused_custom_concepts(valid_vocabs)
        self.drop_unused_custom_vocabularies(valid_vocabs)
        valid_classes = self.get_list_of_valid_classes()
        self.drop_unused_custom_classes(valid_classes)
