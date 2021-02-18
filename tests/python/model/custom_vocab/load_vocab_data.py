from src.delphyne import Wrapper

from tests.python.cdm import cdm600


def load_minimal_vocabulary(wrapper: Wrapper) -> None:
    """Load the bare minimum records into the vocabulary tables."""
    with wrapper.db.session_scope() as session:

        vocab = cdm600.Vocabulary()
        vocab.vocabulary_id = 'ARTIFICIAL_VOCAB'
        vocab.vocabulary_name = 'Artificial vocab'
        vocab.vocabulary_reference = 'vocab ref'
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
