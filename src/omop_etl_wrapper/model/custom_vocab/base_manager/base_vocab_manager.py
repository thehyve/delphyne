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

    def _get_new_custom_vocabulary_ids(self) -> List[str]:
        # create a list of custom vocabulary ids
        # from the custom vocabulary table if the same vocabulary
        # version is not already present in the database

        logging.info('Looking for new custom vocabulary versions')

        vocab_ids = set()

        for vocab_file in self._custom_vocab_files:

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
        # For a given custom vocabulary id,
        # retrieve the version already present in the database
        # if available, otherwise None

        with self.db.session_scope() as session:
            existing_record = \
                session.query(self._cdm.Vocabulary) \
                .filter(self._cdm.Vocabulary.vocabulary_id == vocab_id) \
                .one_or_none()
            return existing_record.vocabulary_version if existing_record is not None else None

    def _drop_custom_vocabularies(self, vocab_ids: List[str]) -> None:
        # Drop a list of custom vocabulary ids from the database

        logging.info(f'Dropping old custom vocabulary versions: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.Vocabulary) \
                    .filter(self._cdm.Vocabulary.vocabulary_id.in_(vocab_ids)) \
                    .delete(synchronize_session=False)

    def _load_custom_vocabularies(self, vocab_ids: List[str]) -> None:
        # Load a list of custom vocabularies to the database

        logging.info(f'Loading new custom vocabulary versions: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:

            with self.db.session_scope() as session:

                records = []

                for vocab_file in self._custom_vocab_files:
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
