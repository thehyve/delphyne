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
        skip_standard_vocabs = config.run_options.skip_vocabulary_loading
        skip_custom_vocabs = config.run_options.skip_custom_vocabulary_loading
        skip_stcm = config.run_options.skip_source_to_concept_map_loading

        self.standard_vocabularies = StandardVocabLoader(db, cdm, skip_standard_vocabs)
        self.custom_vocabularies = CustomVocabLoader(db, cdm, skip_custom_vocabs)
        self.stcm = StcmLoader(db, cdm, skip_stcm)
