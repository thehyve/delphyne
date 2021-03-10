from typing import List, Dict, Tuple

from src.delphyne import Wrapper

from tests.python.cdm import cdm600


def load_custom_vocab_records(wrapper: Wrapper, vocab_ids: List[str]) -> None:
    """Add custom vocabulary records."""
    with wrapper.db.session_scope() as session:
        for vocab_id in vocab_ids:
            vocab = cdm600.Vocabulary()
            vocab.vocabulary_id = vocab_id
            vocab.vocabulary_name = vocab_id
            vocab.vocabulary_reference = vocab_id
            vocab.vocabulary_version = vocab_id + '_v1'
            vocab.vocabulary_concept_id = 0
            session.add(vocab)


def load_custom_class_records(wrapper: Wrapper, class_ids: List[str]) -> None:
    """Add custom concept_class records."""
    with wrapper.db.session_scope() as session:
        for class_id in class_ids:
            concept_class = cdm600.ConceptClass()
            concept_class.concept_class_id = class_id
            concept_class.concept_class_name = class_id + '_v1'
            concept_class.concept_class_concept_id = 0
            session.add(concept_class)


def load_custom_concept_records(wrapper: Wrapper, concepts: Dict[int, Tuple[str, str]]) -> None:
    """Add custom concept records. concepts is a dictionary of
    concept_id : (vocab_id, concept_class) dicts """
    with wrapper.db.session_scope() as session:
        for concept_id, values in concepts.items():
            vocab_id, class_id = values[0], values[1]
            concept = cdm600.Concept()
            concept.concept_id = concept_id
            concept.vocabulary_id = vocab_id
            concept.concept_name = concept_id
            concept.concept_code = concept_id
            concept.concept_class_id = class_id if class_id else 'ARTIFICIAL_CLASS'
            concept.domain_id = 'ARTIFICIAL_DOMAIN'
            concept.valid_start_date = '1970-01-01'
            concept.valid_end_date = '2099-12-31'
            concept.standard_concept = None
            concept.invalid_reason = None
            session.add(concept)


def load_minimal_vocabulary(wrapper: Wrapper) -> None:
    """Load the bare minimum records into the vocabulary tables."""
    with wrapper.db.session_scope() as session:

        vocab = cdm600.Vocabulary()
        vocab.vocabulary_id = 'ARTIFICIAL_VOCAB'
        vocab.vocabulary_name = 'Artificial vocab'
        vocab.vocabulary_reference = 'vocab ref'
        vocab.vocabulary_version = 'v1'
        vocab.vocabulary_concept_id = 1

        conc_class = cdm600.ConceptClass()
        conc_class.concept_class_id = 'ARTIFICIAL_CLASS'
        conc_class.concept_class_name = 'Artificial Class'
        conc_class.concept_class_concept_id = 1

        domain = cdm600.Domain()
        domain.domain_id = 'ARTIFICIAL_DOMAIN'
        domain.domain_name = 'Artificial Domain'
        domain.domain_concept_id = 1

        concept = cdm600.Concept()
        concept.concept_id = 0
        concept.concept_name = 'CUSTOM_ENTITY_CONCEPT'
        concept.domain_id = domain.domain_id
        concept.vocabulary_id = vocab.vocabulary_id
        concept.concept_class_id = conc_class.concept_class_id
        concept.concept_code = 'Concept for custom entities'
        concept.valid_start_date = '1970-01-01'
        concept.valid_end_date = '2099-12-31'

        # Artificial entities need to have concept_id other than 0,
        # otherwise they will be interpreted as custom entities
        # and dropped as "obsolete" if not in user-provided tables
        concept1 = cdm600.Concept()
        concept1.concept_id = 1
        concept1.concept_name = 'ARTIFICIAL_CONCEPT'
        concept1.domain_id = domain.domain_id
        concept1.vocabulary_id = vocab.vocabulary_id
        concept1.concept_class_id = conc_class.concept_class_id
        concept1.concept_code = 'Concept for artificial entities'
        concept1.valid_start_date = '1970-01-01'
        concept1.valid_end_date = '2099-12-31'

        session.add_all([vocab, conc_class, domain, concept, concept1])
