"""Vocabulary management."""

import logging

from .custom_vocab import CustomVocabLoader
from .standard_vocab import StandardVocabLoader
from .stcm import StcmLoader
from ..config.models import MainConfig
from ..database import Database

logger = logging.getLogger(__name__)


class VocabManager:
    """
    Composition class for vocabulary loading/management.

    Parameters
    ----------
    db : Database
        Database instance to interact with.
    cdm : module
        Module containing all CDM table definitions.
    config : MainConfig
        Config instance including fields that indicate whether certain
        vocabulary loading steps should be ignored.
    """

    def __init__(self, db: Database, cdm, config: MainConfig):
        self._db = db
        self._cdm = cdm

        self._load_standard_vocabs = config.run_options.load_vocabulary
        self._load_custom_vocabs = config.run_options.load_custom_vocabulary
        self._load_stcm = config.run_options.load_source_to_concept_map

        self.standard_vocabularies = StandardVocabLoader(db, cdm, self._load_standard_vocabs)
        self.custom_vocabularies = CustomVocabLoader(db, cdm, self._load_custom_vocabs)
        self.stcm = StcmLoader(db, cdm, self._load_stcm)

    def load_custom_vocab_and_stcm_tables(self) -> None:
        """
        Process any updates in custom vocabulary/STCM files.

        Any new version provided in either the custom vocabulary folder,
        or the STCM folder will be parsed and updated in the respective
        tables.

        Returns
        -------
        None
        """
        stcm_table_name = self._cdm.SourceToConceptMap.__tablename__
        stcm_version_table_name = self._cdm.SourceToConceptMapVersion.__tablename__

        # Temporarily drop STCM tables FKs only when the custom
        # vocabulary loading is enabled (to avoid FK violations)
        if self._load_custom_vocabs:
            self._drop_stcm_fks(stcm_table_name, stcm_version_table_name)
        self.custom_vocabularies.load()
        self.stcm.load()
        if self._load_custom_vocabs:
            self._add_stcm_fks(stcm_table_name, stcm_version_table_name)

    def _drop_stcm_fks(self, stcm_table: str, stcm_version_table: str):
        self._db.constraint_manager.drop_table_constraints(table_name=stcm_table,
                                                           drop_pk=False, drop_index=False)
        self._db.constraint_manager.drop_table_constraints(stcm_version_table,
                                                           drop_pk=False, drop_index=False)

    def _add_stcm_fks(self, stcm_table: str, stcm_version_table: str):
        self._db.constraint_manager.add_table_constraints(table_name=stcm_table,
                                                          add_pk=False, add_index=False)
        self._db.constraint_manager.add_table_constraints(stcm_version_table,
                                                          add_pk=False, add_index=False)
