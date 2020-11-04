from pathlib import Path
from typing import List

from .._paths import CUSTOM_VOCAB_DIR
from ..database import Database
from ..util.io import is_hidden
import pandas as pd
import numpy as np
import logging


logger = logging.getLogger(__name__)


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

    def _get_existing_vocab_versions(self):
        with self.db.session_scope() as session:
            vocabs = session.query(self._cdm.Vocabulary).all()
            return {v.vocabulary_id: v.vocabulary_version for v in vocabs}

    def load_custom_vocabulary_tables(self):

        # TODO: quality checks: mandatory fields, dependencies
        # self.check_custom_vocabularies_format()

        # TODO: not sure I need this for anything, see how it is implemented in
        #  check_if_existing_vocab_version(), check_if_existing_custom_class()
        # existing_vocabs = self._get_existing_vocab_versions()

        # get vocabularies and classes that need to be updated
        vocab_ids, vocab_files = self._get_new_custom_vocabulary_ids_and_files()
        class_ids, class_files = self._get_custom_class_ids_and_files()

        # TODO: remove/reapply constraints where needed
        # TODO: drop/load custom vocabulary concept id, if != 0
        # drop older version
        self._drop_custom_concepts(vocab_ids)
        self._drop_custom_vocabularies(vocab_ids)
        self._drop_custom_classes(class_ids)
        # load new version
        self._load_custom_classes(class_ids, class_files)
        self._load_custom_vocabularies(vocab_ids, vocab_files)
        self._load_custom_concepts(vocab_ids)
        # TODO: remove obsolete versions (i.e. cleanup in case of renaming of vocabs/classes);
        #  if the name has been changed, the previous drop won't find them;
        #  NOTE: for this to work, you need to keep a list of valid Athena vocabulary ids
        #  and check that no unknown vocabulary is present (not in Athena or custom vocab files);
        #  the cleanup could be rather time-consuming and should not be executed every time
        valid_vocabs = self._get_list_of_valid_vocabularies()
        self._drop_unused_custom_concepts(valid_vocabs)
        self._drop_unused_custom_vocabularies(valid_vocabs)
        valid_classes = self._get_list_of_valid_classes()
        self._drop_unused_custom_classes(valid_classes)

    def _get_new_custom_vocabulary_ids_and_files(self):

        logging.info('Looking for new custom vocabulary versions')

        vocab_ids = set()
        vocab_files = set()

        for vocab_file in self._subset_custom_vocab_files('vocabulary'):

            df = pd.read_csv(vocab_file, sep='\t')
            for _, row in df.iterrows():
                vocab_id = row['vocabulary_id']
                vocab_version = row['vocabulary_version']

                # skip loading if vocabulary version already present
                if self._check_if_existing_vocab_version(vocab_id, vocab_version):
                    continue

                logging.info(f'Found vocabulary: {vocab_id, vocab_version}')

                vocab_ids.add(vocab_id)
                vocab_files.add(vocab_file)

        if not vocab_ids:
            logging.info('No new vocabulary version found')

        return list(vocab_ids), list(vocab_files)

    def _check_if_existing_vocab_version(self, vocab_id, vocab_version):

        with self.db.session_scope() as session:
            existing_record = \
                session.query(self._cdm.Vocabulary) \
                    .filter(self._cdm.Vocabulary.vocabulary_id == vocab_id) \
                    .filter(self._cdm.Vocabulary.vocabulary_version == vocab_version) \
                    .one_or_none()
            return False if not existing_record else True

    def _get_custom_class_ids_and_files(self):

        logging.info('Looking for new custom class versions')

        class_ids = set()
        class_files = set()

        for class_file in self._subset_custom_vocab_files('concept_class'):
            df = pd.read_csv(class_file, sep='\t')
            for _, row in df.iterrows():
                class_id = row['concept_class_id']
                class_name = row['concept_class_name']
                class_concept_id = int(row['concept_class_concept_id'])

                # skip loading if class version already present
                if self._check_if_existing_custom_class(class_id, class_name, class_concept_id):
                    continue

                logging.info(f'Found class: {class_id, class_name, class_concept_id}')

                class_ids.add(class_id)
                class_files.add(class_file)

        if not class_ids:
            logging.info('No new class version found')

        return list(class_ids), list(class_files)

    def _check_if_existing_custom_class(self, class_id, class_name, class_concept_id):

        with self.db.session_scope() as session:
            existing_record = \
                session.query(self._cdm.ConceptClass) \
                .filter(self._cdm.ConceptClass.concept_class_id == class_id) \
                .filter(self._cdm.ConceptClass.concept_class_name == class_name) \
                .filter(self._cdm.ConceptClass.concept_class_concept_id == class_concept_id) \
                .one_or_none()
            return False if not existing_record else True

    def _drop_custom_concepts(self, vocab_ids):

        logging.info(f'Dropping old custom concepts: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.Concept) \
                    .filter(self._cdm.Concept.vocabulary_id.in_(vocab_ids)) \
                    .delete(synchronize_session='fetch')

    def _drop_custom_vocabularies(self, vocab_ids):

        logging.info(f'Dropping old custom vocabulary versions: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.Vocabulary) \
                    .filter(self._cdm.Vocabulary.vocabulary_id.in_(vocab_ids)) \
                    .delete(synchronize_session='fetch')

    def _drop_custom_classes(self, class_ids):

        logging.info(f'Dropping old custom concept class versions: '
                     f'{True if class_ids else False}')

        if class_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.ConceptClass) \
                    .filter(self._cdm.ConceptClass.concept_class_id.in_(class_ids)) \
                    .delete(synchronize_session='fetch')

    def _load_custom_classes(self, class_ids, class_files):

        logging.info(f'Loading new custom class versions: '
                     f'{True if class_ids else False}')

        if class_ids:

            with self.db.session_scope() as session:

                for class_file in class_files:
                    df = pd.read_csv(class_file, sep='\t')
                    df = df[df['concept_class_id'].isin(class_ids)]

                    records = []
                    for _,row in df.iterrows():
                        records.append(self._cdm.ConceptClass(
                            concept_class_id=row['concept_class_id'],
                            concept_class_name=row['concept_class_name'],
                            concept_class_concept_id=row['concept_class_concept_id']
                        ))
                    session.add_all(records)

    def _load_custom_vocabularies(self, vocab_ids, vocab_files):

        logging.info(f'Loading new custom vocabulary versions: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:

            with self.db.session_scope() as session:

                for vocab_file in vocab_files:
                    df = pd.read_csv(vocab_file, sep='\t')
                    df = df[df['vocabulary_id'].isin(vocab_ids)]

                    records = []
                    for _, row in df.iterrows():
                        records.append(self._cdm.Vocabulary(
                            vocabulary_id=row['vocabulary_id'],
                            vocabulary_name=row['vocabulary_name'],
                            vocabulary_reference=row['vocabulary_reference'],
                            vocabulary_version=row['vocabulary_version'],
                            vocabulary_concept_id=row['vocabulary_concept_id']
                        ))
                    session.add_all(records)

    def _load_custom_concepts(self, vocab_ids):

        logging.info(f'Loading new custom concept_ids: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:

            with self.db.session_scope() as session:

                for concept_file in self._subset_custom_vocab_files('concept'):

                    df = pd.read_csv(concept_file, sep='\t')
                    df = df[df['vocabulary_id'].isin(vocab_ids)]
                    df = df.replace({np.nan: None})

                    records = []
                    for _, row in df.iterrows():
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

    def _get_list_of_valid_vocabularies(self):
        pass

    def _get_list_of_valid_classes(self):
        pass

    def _drop_unused_custom_concepts(self, vocab_ids):
        pass

    def _drop_unused_custom_vocabularies(self, vocab_ids):
        pass

    def _drop_unused_custom_classes(self, class_ids):
        pass