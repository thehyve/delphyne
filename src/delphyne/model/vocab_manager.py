import logging

from .custom_vocab import CustomVocabLoader
from .standard_vocab import StandardVocabLoader
from .stcm import StcmLoader
from ..config.models import MainConfig
from ..database import Database

logger = logging.getLogger(__name__)


class VocabManager:
    def __init__(self, db: Database, cdm, config: MainConfig):
        self._custom_vocab_loader = CustomVocabLoader(db, cdm)
        self._standard_vocab_loader = StandardVocabLoader(db, cdm)
        self._stcm_loader = StcmLoader(db, cdm)

        self._load_standard_vocabs = not config.run_options.skip_vocabulary_loading
        self._load_custom_vocabs = not config.run_options.skip_custom_vocabulary_loading
        self._load_stcm = not config.run_options.skip_source_to_concept_map_loading

    def load_custom_vocabularies(self):
        """
        Loads custom vocabularies to the vocabulary schema.

        - Checks for the presence of custom vocabularies and
          concept_classes at a predefined folder location;
        - Compares the version of custom vocabularies and
          concept_classes in the folder to that of custom vocabularies
          and tables already present in the database;
        - Deletes obsolete versions from the database;
        - Loads the new versions to the database.

        :return: None
        """
        logger.info(f'Loading custom vocabulary tables: {self._load_custom_vocabs}')
        if self._load_custom_vocabs:
            self._custom_vocab_loader.load_custom_vocabulary_tables()

    def load_standard_vocabularies(self):
        """
        Insert all Athena vocabulary files into the vocabulary tables.

        All vocabulary tables must be empty before any data can be
        inserted. All constraints and indexes will be dropped before
        insertion and restored afterwards.

        :return: None
        """
        logger.info(f'Loading standard vocabularies: {self._load_standard_vocabs}')
        if self._load_standard_vocabs:
            self._standard_vocab_loader.load_standard_vocabs()

    def load_stcm(self):
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

        :return: None
        """
        logger.info(f'Loading source_to_concept_map files: {self._load_stcm}')
        if self._load_stcm:
            self._stcm_loader.load_stcm()
