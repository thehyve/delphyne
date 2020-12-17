import logging

from .custom_vocab import CustomVocabLoader
from .stcm import StcmLoader
from ..config.models import MainConfig
from ..database import Database

logger = logging.getLogger(__name__)


class VocabManager:
    def __init__(self, db: Database, cdm, config: MainConfig):
        self._custom_vocab_loader = CustomVocabLoader(db, cdm)
        self._stcm_loader = StcmLoader(db, cdm)

        self._load_custom_vocabs = not config.run_options.skip_custom_vocabulary_loading
        self._load_stcm = not config.run_options.skip_source_to_concept_map_loading

    def load_custom_vocabularies(self):
        """
        Loads custom vocabularies to the vocabulary schema.

        1. Checks for the presence of custom vocabularies and
        concept_classes at a predefined folder location;
        2. Compares the version of custom vocabularies and
        concept_classes in the folder to that of custom vocabularies
        and tables already present in the database;
        3. Deletes obsolete versions from the database;
        4. Loads the new versions to the database.

        :return: None
        """
        logger.info(f'Loading custom vocabulary tables: {self._load_custom_vocabs}')
        if self._load_custom_vocabs:
            self._custom_vocab_loader.load_custom_vocabulary_tables()

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
