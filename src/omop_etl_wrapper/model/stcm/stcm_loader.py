import csv
import logging
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, Set, Optional

from sqlalchemy import MetaData
from sqlalchemy.exc import InvalidRequestError

from ..._paths import STCM_DIR, STCM_VERSION_FILE
from ...cdm._schema_placeholders import VOCAB_SCHEMA
from ...cdm.vocabularies import BaseSourceToConceptMapVersion
from ...database import Database
from ...model.etl_stats import EtlTransformation, EtlStats
from ...util.io import is_hidden

logger = logging.getLogger(__name__)

_STCM_VERSION_TABLE_NAME = BaseSourceToConceptMapVersion.__tablename__


class StcmLoader:
    def __init__(self, db: Database, cdm, etl_stats: EtlStats):
        self._db = db
        self._cdm = cdm
        self._etl_stats = etl_stats
        self._loaded_stcm_versions: Dict[str, str] = {}
        self._new_stcm_versions: Dict[str, str] = {}

    @property
    @lru_cache()
    def _stcm_vocabs_to_update(self) -> Set[str]:
        return {vocab_id for vocab_id, version in self._new_stcm_versions.items()
                if version != self._loaded_stcm_versions.get(vocab_id)}

    @property
    @lru_cache()
    def _loaded_vocabulary_ids(self) -> Set[str]:
        with self._db.session_scope() as session:
            records = session.query(self._cdm.Vocabulary.vocabulary_id).all()
            return {vocabulary_id for vocabulary_id, in records}

    def load_stcm(self):
        logger.info('Loading STCM files')
        if not STCM_DIR.exists():
            raise FileNotFoundError(f'{STCM_DIR.resolve()} folder not found')
        self._get_loaded_stcm_versions()
        self._get_provided_stcm_versions()
        self._delete_outdated_stcm_records()

        stcm_files = self._get_stcm_files()
        for stcm_file in stcm_files:
            if not self._must_be_loaded(stcm_file):
                logger.info(f'Skipping file {stcm_file.name} as this STCM file '
                            f'has no new version available.')
                continue
            self._load_stcm_from_file(stcm_file)
        self._update_stcm_version_table()

    @staticmethod
    def _get_stcm_files() -> Set[Path]:
        files = set()
        for f in STCM_DIR.glob('*'):
            if is_hidden(f) or not f.is_file() or f.name == STCM_VERSION_FILE.name:
                continue
            files.add(f)
        return files

    def _get_loaded_stcm_versions(self) -> None:
        self._check_stcm_version_table_exists()
        with self._db.session_scope() as session:
            result = session.query(self._cdm.SourceToConceptMapVersion).all()
            vocab_version_dict = {r.source_vocabulary_id: r.stcm_version for r in result}
            self._loaded_stcm_versions = vocab_version_dict

    def _get_provided_stcm_versions(self) -> None:
        with STCM_VERSION_FILE.open('r') as version_file:
            reader = csv.DictReader(version_file, delimiter='\t')
            for line in reader:
                vocab_id = line['source_vocabulary_id']
                version = line['stcm_version']
                if not vocab_id or not version:
                    raise ValueError(f'{STCM_VERSION_FILE.name} may not contain empty values')
                self._new_stcm_versions[vocab_id] = version

    def _check_stcm_version_table_exists(self):
        metadata = MetaData(bind=self._db.engine)
        schema = self._db.schema_translate_map.get(VOCAB_SCHEMA)
        try:
            metadata.reflect(schema=schema, only=[_STCM_VERSION_TABLE_NAME])
        except InvalidRequestError:
            logger.error(f'{schema}.{_STCM_VERSION_TABLE_NAME} does not exist. '
                         f'Run create_all to ensure all required tables are present.')
            raise

    def _must_be_loaded(self, stcm_file: Path) -> bool:
        stcm_file_vocab_id = self._get_stcm_file_vocab_id(stcm_file)
        if stcm_file_vocab_id is None:
            return True
        if stcm_file_vocab_id in self._stcm_vocabs_to_update:
            return True
        return False

    def _get_stcm_file_vocab_id(self, stcm_file: Path) -> Optional[str]:
        stem_name = stcm_file.stem
        if stem_name.endswith('_stcm'):
            vocab_id = stem_name.rsplit('_')[0]
            if vocab_id in self._new_stcm_versions:
                return vocab_id
        return None

    def _delete_outdated_stcm_records(self) -> None:
        if not self._stcm_vocabs_to_update:
            return
        logger.info(f'Deleting STCM records for vocabulary_ids: {self._stcm_vocabs_to_update}')
        with self._db.session_scope() as session:
            stcm_table = self._cdm.SourceToConceptMap
            q = session.query(stcm_table)
            q = q.filter(stcm_table.source_vocabulary_id.in_(self._stcm_vocabs_to_update))
            q.delete(synchronize_session=False)

    def _update_stcm_version_table(self):
        with self._db.session_scope() as session:
            stcm_version_table = self._cdm.SourceToConceptMapVersion
            q = session.query(stcm_version_table)
            q = q.filter(stcm_version_table.source_vocabulary_id.in_(self._stcm_vocabs_to_update))
            q.delete(synchronize_session=False)

        with self._db.session_scope() as session:
            for vocab_id in self._stcm_vocabs_to_update:
                r = self._cdm.SourceToConceptMapVersion()
                r.source_vocabulary_id = vocab_id
                r.stcm_version = self._new_stcm_versions[vocab_id]
                session.add(r)

    # def truncate_stcm_tables(self):
    #     """Delete all records in the source_to_concept_map table."""
    #     # TODO: truncate version table as well
    #     logger.info('Truncating STCM table')
    #     with self._db.session_scope() as session:
    #         session.query(self._cdm.SourceToConceptMap).delete()

    def _load_stcm_from_file(self, stcm_file: Path) -> None:
        """
        Insert STCM file into the STCM vocabulary table and add
        contents to stcm_lookup.

        :param stcm_file: Path
            Separated values file with header matching the CDM STCM
            columns
        :return: None
        """
        logger.info(f'Loading STCM file: {stcm_file.name}')
        transformation_metadata = EtlTransformation(name=f'load_{stcm_file.stem}')
        with self._db.session_scope(metadata=transformation_metadata) as session, \
                stcm_file.open('r') as f_in:
            rows = csv.DictReader(f_in)

            for i, row in enumerate(rows, start=2):
                source_vocabulary_id = row['source_vocabulary_id']
                if source_vocabulary_id not in self._stcm_vocabs_to_update:
                    continue
                # Skip unmapped records
                target_concept_id = int(row['target_concept_id'])
                if target_concept_id == 0:
                    continue
                if source_vocabulary_id not in self._loaded_vocabulary_ids:
                    raise ValueError(f'Cannot insert line {i} of {stcm_file.name}. '
                                     f'{source_vocabulary_id} is not in the vocabulary table')
                session.add(self._cdm.SourceToConceptMap(**row))

            transformation_metadata.end = datetime.now()
            self._etl_stats.add_transformation(transformation_metadata)
