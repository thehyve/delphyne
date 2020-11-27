from pathlib import Path
from typing import List, Union
import csv

from .._paths import CUSTOM_VOCAB_DIR
from ..database import Database
from ..util.io import is_hidden
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

    def load_custom_vocabulary_tables(self) -> None:

        # TODO: quality checks: mandatory fields, dependencies;
        #  warn if overriding standard Athena vocabulary name
        # self.check_custom_vocabularies_format()

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

    def _get_new_custom_vocabulary_ids(self) -> List[str]:

        logging.info('Looking for new custom vocabulary versions')

        vocab_ids = set()

        for vocab_file in self._subset_custom_vocab_files('vocabulary'):

            with open(vocab_file) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    vocab_id = row['vocabulary_id']
                    vocab_version = row['vocabulary_version']

                    old_vocab_version = self._get_old_vocab_version(vocab_id)

                    # skip loading if vocabulary version already present
                    if vocab_version == old_vocab_version:
                        continue

                    logging.info(f'Found new vocabulary version: {vocab_id} : '
                                 f'{old_vocab_version} ->  {vocab_version}')
                    vocab_ids.add(vocab_id)

        if not vocab_ids:
            logging.info('No new vocabulary version found')

        return list(vocab_ids)

    def _get_old_vocab_version(self, vocab_id: str) -> Union[bool, None]:

        with self.db.session_scope() as session:
            existing_record = \
                session.query(self._cdm.Vocabulary) \
                .filter(self._cdm.Vocabulary.vocabulary_id == vocab_id) \
                .one_or_none()
            return existing_record.vocabulary_version if existing_record is not None else None

    def _get_new_custom_concept_class_ids(self) -> List[str]:

        logging.info('Looking for new custom class versions')

        class_ids = set()

        for class_file in self._subset_custom_vocab_files('concept_class'):

            with open(class_file) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    class_id = row['concept_class_id']
                    class_name = row['concept_class_name']

                    old_class_name = self._get_old_class_version(class_id)

                    # skip loading if class version already present
                    if class_name == old_class_name:
                        continue

                    logging.info(f'Found new class name: {class_id} : '
                                 f'{old_class_name} ->  {class_name}')
                    class_ids.add(class_id)

        if not class_ids:
            logging.info('No new class version found')

        return list(class_ids)

    def _get_old_class_version(self, class_id: str) -> Union[bool, None]:

        with self.db.session_scope() as session:
            existing_record = \
                session.query(self._cdm.ConceptClass) \
                .filter(self._cdm.ConceptClass.concept_class_id == class_id) \
                .one_or_none()
            return existing_record.concept_class_name if existing_record is not None else None

    def _drop_custom_concepts(self, vocab_ids: List[str]) -> None:

        logging.info(f'Dropping old custom concepts: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.Concept) \
                    .filter(self._cdm.Concept.vocabulary_id.in_(vocab_ids)) \
                    .delete(synchronize_session=False)

    def _drop_custom_vocabularies(self, vocab_ids: List[str]) -> None:

        logging.info(f'Dropping old custom vocabulary versions: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.Vocabulary) \
                    .filter(self._cdm.Vocabulary.vocabulary_id.in_(vocab_ids)) \
                    .delete(synchronize_session=False)

    def _drop_custom_classes(self, class_ids: List[str]) -> None:

        logging.info(f'Dropping old custom concept class versions: '
                     f'{True if class_ids else False}')

        if class_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.ConceptClass) \
                    .filter(self._cdm.ConceptClass.concept_class_id.in_(class_ids)) \
                    .delete(synchronize_session=False)

    def _load_custom_classes(self, class_ids: List[str]) -> None:

        logging.info(f'Loading new custom class versions: '
                     f'{True if class_ids else False}')

        if class_ids:

            with self.db.session_scope() as session:

                records = []

                for class_file in self._subset_custom_vocab_files('concept_class'):
                    with open(class_file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        for row in reader:
                            if row['concept_class_id'] in class_ids:
                                records.append(self._cdm.ConceptClass(
                                    concept_class_id=row['concept_class_id'],
                                    concept_class_name=row['concept_class_name'],
                                    concept_class_concept_id=row['concept_class_concept_id']
                                ))

                session.add_all(records)

    def _load_custom_vocabularies(self, vocab_ids: List[str]) -> None:

        logging.info(f'Loading new custom vocabulary versions: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:

            with self.db.session_scope() as session:

                records = []

                for vocab_file in self._subset_custom_vocab_files('vocabulary'):
                    with open(vocab_file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        for row in reader:
                            if row['vocabulary_id'] in vocab_ids:
                                records.append(self._cdm.Vocabulary(
                                    vocabulary_id=row['vocabulary_id'],
                                    vocabulary_name=row['vocabulary_name'],
                                    vocabulary_reference=row['vocabulary_reference'],
                                    vocabulary_version=row['vocabulary_version'],
                                    vocabulary_concept_id=row['vocabulary_concept_id']
                                ))
                session.add_all(records)

    def _load_custom_concepts(self, vocab_ids: List[str]) -> None:

        logging.info(f'Loading new custom concept_ids: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:

            with self.db.session_scope() as session:

                records = []

                for concept_file in self._subset_custom_vocab_files('concept'):

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
