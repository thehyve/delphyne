import csv
import logging
from pathlib import Path
from typing import List, Union

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
        self._vocab_ids_update = set()
        self._vocab_ids_all = set()

    def _get_new_custom_vocabulary_ids(self) -> None:
        # Compare custom vocabulary ids
        # to the ones already present in the database.
        #
        # Retrieves:
        # - set of custom vocabularies to be updated
        #   (new id, or same id and new version)
        # - set of all user-provided custom vocabularies

        logging.info('Looking for new custom vocabulary versions')

        for vocab_file in self._custom_vocab_files:

            with open(vocab_file) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    vocab_id = row['vocabulary_id']
                    vocab_version = row['vocabulary_version']

                    self._vocab_ids_all.add(vocab_id)

                    old_vocab_version = self._get_old_vocab_version(vocab_id)

                    # skip loading if vocabulary version already present
                    if vocab_version == old_vocab_version:
                        continue

                    logging.info(f'Found new vocabulary version: {vocab_id} : '
                                 f'{old_vocab_version} ->  {vocab_version}')
                    self._vocab_ids_update.add(vocab_id)

        if not self._vocab_ids_update:
            logging.info('No new vocabulary version found')

    def _get_old_vocab_version(self, vocab_id: str) -> Union[bool, None]:
        # For a given custom vocabulary id, retrieve the version
        # already present in the database if available, otherwise None

        with self.db.session_scope() as session:
            existing_record = \
                session.query(self._cdm.Vocabulary) \
                .filter(self._cdm.Vocabulary.vocabulary_id == vocab_id) \
                .one_or_none()
            return existing_record.vocabulary_version if existing_record is not None else None

    def _drop_updated_custom_vocabs(self) -> None:
        # Drop updated custom vocabulary ids from the database

        logging.info(f'Dropping updated custom vocabulary versions: '
                     f'{True if self._vocab_ids_update else False}')

        if self._vocab_ids_update:
            with self.db.session_scope() as session:
                session.query(self._cdm.Vocabulary) \
                    .filter(self._cdm.Vocabulary.vocabulary_id.in_(self._vocab_ids_update)) \
                    .delete(synchronize_session=False)

    def _load_custom_vocabs(self) -> None:
        # Load new and updated custom vocabularies to the database

        logging.info(f'Loading new custom vocabulary versions: '
                     f'{True if self._vocab_ids_update else False}')

        if self._vocab_ids_update:

            with self.db.session_scope() as session:

                records = []

                for vocab_file in self._custom_vocab_files:
                    with open(vocab_file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        for row in reader:
                            if row['vocabulary_id'] in self._vocab_ids_update:
                                records.append(self._cdm.Vocabulary(
                                    vocabulary_id=row['vocabulary_id'],
                                    vocabulary_name=row['vocabulary_name'],
                                    vocabulary_reference=row['vocabulary_reference'],
                                    vocabulary_version=row['vocabulary_version'],
                                    vocabulary_concept_id=row['vocabulary_concept_id']
                                ))
                session.add_all(records)

    def _drop_unused_custom_vocabs(self) -> None:
        # Drop obsolete custom vocabularies from the database;
        # these are assumed to be all vocabularies in the database with
        # concept_id == 0 minus all user-provided custom vocabularies.

        logging.info(f'Checking for obsolete custom vocabulary versions')

        if self._vocab_ids_all:
            with self.db.session_scope() as session:

                query_base = session.query(self._cdm.Vocabulary) \
                    .filter(self._cdm.Vocabulary.vocabulary_concept_id == 0) \
                    .filter(self._cdm.Vocabulary.vocabulary_id.notin_(self._vocab_ids_all))

                record = query_base.one_or_none()
                if record:
                    logging.info(f'Dropping unused custom vocabulary: {record.vocabulary_id}')
                    query_base.delete(synchronize_session=False)
