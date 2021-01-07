from typing import List

from src.delphyne import Wrapper

from tests.python.cdm import cdm600


def load_custom_vocab_records(wrapper: Wrapper, vocab_ids: List[str]) -> None:
    with wrapper.db.session_scope() as session:
        for vocab_id in vocab_ids:
            vocab = cdm600.Vocabulary()
            vocab.vocabulary_id = vocab_id
            vocab.vocabulary_name = vocab_id
            vocab.vocabulary_reference = vocab_id
            vocab.vocabulary_concept_id = 0

            session.add(vocab)


def load_minimal_vocabulary(wrapper: Wrapper) -> None:
    """Load the bare minimum records into the vocabulary tables."""
    with wrapper.db.session_scope() as session:
        vocab = cdm600.Vocabulary()
        vocab.vocabulary_id = 'ARTIFICIAL_VOCAB'
        vocab.vocabulary_name = 'Artificial Vocabulary'
        vocab.vocabulary_reference = 'vocab ref'
        vocab.vocabulary_concept_id = 0

        conc_class = cdm600.ConceptClass()
        conc_class.concept_class_id = 'ARTIFICIAL_CLASS'
        conc_class.concept_class_name = 'Artificial Class'
        conc_class.concept_class_concept_id = 0

        domain = cdm600.Domain()
        domain.domain_id = 'ARTIFICIAL_DOMAIN'
        domain.domain_name = 'Artificial Domain'
        domain.domain_concept_id = 0

        concept = cdm600.Concept()
        concept.concept_id = 0
        concept.concept_name = 'No Mapping'
        concept.domain_id = domain.domain_id
        concept.vocabulary_id = vocab.vocabulary_id
        concept.concept_class_id = conc_class.concept_class_id
        concept.concept_code = '0'
        concept.valid_start_date = '1970-01-01'
        concept.valid_end_date = '2099-12-31'

        concept2 = cdm600.Concept()
        concept2.concept_id = 1
        concept2.concept_name = 'Concept Name'
        concept2.domain_id = domain.domain_id
        concept2.vocabulary_id = vocab.vocabulary_id
        concept2.concept_class_id = conc_class.concept_class_id
        concept2.concept_code = '1'
        concept2.valid_start_date = '1970-01-01'
        concept2.valid_end_date = '2099-12-31'

        session.add_all([vocab, conc_class, domain, concept, concept2])
