"""Source to concept map loading."""

import csv
import logging
from collections import Counter
from functools import lru_cache
from pathlib import Path
from typing import Dict, Set

from sqlalchemy import MetaData
from sqlalchemy.exc import InvalidRequestError

from ..._paths import STCM_DIR, STCM_VERSION_FILE
from ...cdm._schema_placeholders import VOCAB_SCHEMA
from ...cdm.vocabularies import BaseSourceToConceptMapVersion
from ...database import Database
from ...util.io import get_all_files_in_dir, file_has_valid_prefix

logger = logging.getLogger(__name__)

_STCM_VERSION_TABLE_NAME = BaseSourceToConceptMapVersion.__tablename__


class StcmLoader:
    """
    Loader of source to concept map data into the respective tables.

    Parameters
    ----------
    db : Database
        Database instance to interact with.
    cdm : module
        Module containing all CDM table definitions.
    block_loading : bool
        If True, calls to load tables will be ignored.
    """

    def __init__(self, db: Database, cdm, block_loading: bool):
        self._db = db
        self._cdm = cdm
        self._block_loading = block_loading
        # STCM versions previously loaded into the db
        self._loaded_stcm_versions: Dict[str, str] = {}
        # Newly provided STCM versions from stcm_versions.tsv
        self._provided_stcm_versions: Dict[str, str] = {}

    @property
    @lru_cache()
    def _stcm_vocabs_to_update(self) -> Set[str]:
        return {vocab_id for vocab_id, version in self._provided_stcm_versions.items()
                if version != self._loaded_stcm_versions.get(vocab_id)}

    @property
    @lru_cache()
    def _loaded_vocabulary_ids(self) -> Set[str]:
        with self._db.session_scope() as session:
            records = session.query(self._cdm.Vocabulary.vocabulary_id).all()
            return {vocabulary_id for vocabulary_id, in records}

    @staticmethod
    def _invalidate_cache() -> None:
        StcmLoader._stcm_vocabs_to_update.fget.cache_clear()
        StcmLoader._loaded_vocabulary_ids.fget.cache_clear()

    def load(self) -> None:
        """
        Load STCM files into the source_to_concept_map table.

        Only new STCM mappings, as specified in stcm_versions.tsv, will
        be inserted. All records in the source_to_concept_map table that
        belong to vocabulary_ids that need updating, will be deleted
        before the new records are inserted.
        If an STCM file contains exclusively records of one
        source_vocabulary_id, it can be named as
        <vocab_id>_stcm.<file_extension> to make sure it will not be
        parsed if no new version is available for that vocabulary.

        Returns
        -------
        None
        """
        logger.info(f'Loading source_to_concept_map files: {not self._block_loading}')
        if self._block_loading:
            return
        self._invalidate_cache()
        if not STCM_DIR.exists():
            raise FileNotFoundError(f'{STCM_DIR.resolve()} folder not found')
        self._get_loaded_stcm_versions()
        self._get_provided_stcm_versions()
        if not self._stcm_vocabs_to_update:
            logger.info('No new STCM versions provided')
            return
        self._delete_outdated_stcm_records()

        stcm_files = self._get_stcm_files()
        vocab_ids_all = set(self._provided_stcm_versions.keys())
        for stcm_file in stcm_files:
            if not file_has_valid_prefix(stcm_file, 'stcm',
                                         all_prefixes=vocab_ids_all,
                                         valid_prefixes=self._stcm_vocabs_to_update):
                logger.info(f'Skipping file {stcm_file.name} as this STCM file '
                            f'has no new version available.')
                continue
            self._load_stcm_from_file(stcm_file)
        self._update_stcm_version_table()

    @staticmethod
    def _get_stcm_files() -> Set[Path]:
        files = get_all_files_in_dir(STCM_DIR)
        return {f for f in files if not f.name == STCM_VERSION_FILE.name}

    def _get_loaded_stcm_versions(self) -> None:
        self._check_stcm_version_table_exists()
        with self._db.session_scope() as session:
            result = session.query(self._cdm.SourceToConceptMapVersion).all()
            vocab_version_dict = {r.source_vocabulary_id: r.stcm_version for r in result}
            self._loaded_stcm_versions = vocab_version_dict

    def _get_provided_stcm_versions(self) -> None:
        if not STCM_VERSION_FILE.exists():
            raise FileNotFoundError('source to concept map version file not found. '
                                    f'Expected file at {STCM_VERSION_FILE}')
        with STCM_VERSION_FILE.open('r') as version_file:
            reader = csv.DictReader(version_file, delimiter='\t')
            for line in reader:
                vocab_id = line['source_vocabulary_id']
                version = line['stcm_version']
                if not vocab_id or not version:
                    raise ValueError(f'{STCM_VERSION_FILE.name} may not contain empty values')
                if vocab_id not in self._loaded_vocabulary_ids:
                    raise ValueError(f'{vocab_id} is not present in the vocabulary table. '
                                     'Make sure to add it as a custom vocabulary.')
                self._provided_stcm_versions[vocab_id] = version

    def _check_stcm_version_table_exists(self) -> None:
        metadata = MetaData(bind=self._db.engine)
        schema = self._db.schema_translate_map.get(VOCAB_SCHEMA)
        try:
            metadata.reflect(schema=schema, only=[_STCM_VERSION_TABLE_NAME])
        except InvalidRequestError:
            logger.error(f'Table {schema}.{_STCM_VERSION_TABLE_NAME} does not exist. '
                         'Run create_all to ensure all required tables are present.')
            raise

    def _delete_outdated_stcm_records(self) -> None:
        # Delete STCM records for all source_vocabulary_ids for which a
        # new version is provided in the stcm version file.
        if not self._stcm_vocabs_to_update:
            return
        logger.info(f'Deleting STCM records for vocabulary_ids: {self._stcm_vocabs_to_update}')
        with self._db.session_scope() as session:
            stcm_table = self._cdm.SourceToConceptMap
            q = session.query(stcm_table)
            q = q.filter(stcm_table.source_vocabulary_id.in_(self._stcm_vocabs_to_update))
            q.delete(synchronize_session=False)

    def _update_stcm_version_table(self) -> None:
        with self._db.session_scope() as session:
            stcm_version_table = self._cdm.SourceToConceptMapVersion
            q = session.query(stcm_version_table)
            q = q.filter(stcm_version_table.source_vocabulary_id.in_(self._stcm_vocabs_to_update))
            q.delete(synchronize_session=False)

        with self._db.session_scope() as session:
            for vocab_id in self._stcm_vocabs_to_update:
                r = self._cdm.SourceToConceptMapVersion()
                r.source_vocabulary_id = vocab_id
                r.stcm_version = self._provided_stcm_versions[vocab_id]
                session.add(r)

    def _load_stcm_from_file(self, stcm_file: Path) -> None:
        logger.info(f'Loading STCM file: {stcm_file.name}')
        with self._db.tracked_session_scope(name=f'load_{stcm_file.stem}') as (session, _), \
                stcm_file.open('r') as f_in:
            rows = csv.DictReader(f_in)
            ignored_vocabs = Counter()
            unrecognized_vocabs = Counter()

            for i, row in enumerate(rows, start=2):
                source_vocabulary_id = row['source_vocabulary_id']
                if source_vocabulary_id not in self._provided_stcm_versions:
                    unrecognized_vocabs.update([source_vocabulary_id])
                    continue
                if source_vocabulary_id not in self._stcm_vocabs_to_update:
                    ignored_vocabs.update([source_vocabulary_id])
                    continue
                # Skip unmapped records
                target_concept_id = int(row['target_concept_id'])
                if target_concept_id == 0:
                    continue
                if source_vocabulary_id not in self._loaded_vocabulary_ids:
                    raise ValueError(f'Cannot insert line {i} of {stcm_file.name}. '
                                     f'{source_vocabulary_id} is not in the vocabulary table')
                session.add(self._cdm.SourceToConceptMap(**row))

            if unrecognized_vocabs:
                logger.warning(f'Skipped records with source_vocabulary_id values that '
                               f'were not present in {STCM_VERSION_FILE.name}: '
                               f'{unrecognized_vocabs.most_common()}')

            if ignored_vocabs:
                logger.info(f'Skipped records with source_vocabulary_id values that '
                            f'were already loaded under the current version: '
                            f'{ignored_vocabs.most_common()}')
