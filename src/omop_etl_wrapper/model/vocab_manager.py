import logging

from .custom_vocab import CustomVocabLoader
from ..config.models import MainConfig
from ..database import Database

logger = logging.getLogger(__name__)


class VocabManager:
    def __init__(self, db: Database, cdm, config: MainConfig):
        self._custom_vocab_loader = CustomVocabLoader(db, cdm)
        self._stcm_loader = None

        self._load_custom_vocabs = not config.run_options.skip_custom_vocabulary_loading

    def load_custom_vocabularies(self):
        logger.info(f'Loading custom vocabulary tables: {self._load_custom_vocabs}')
        if self._load_custom_vocabs:
            self._custom_vocab_loader.load_custom_vocabulary_tables()
