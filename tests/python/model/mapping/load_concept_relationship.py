from src.delphyne import Wrapper

from tests.python.cdm import cdm600


def load_concept_relationship(wrapper: Wrapper) -> None:
    """Load test concept and concept relationship records in the
    vocabulary tables."""
    with wrapper.db.session_scope() as session:

        vocab1 = cdm600.Vocabulary()
        vocab1.vocabulary_id = 'SOURCE'
        vocab1.vocabulary_name = 'Non-standard vocabulary'
        vocab1.vocabulary_reference = 'vocab ref'
        vocab1.vocabulary_concept_id = 0

        vocab2 = cdm600.Vocabulary()
        vocab2.vocabulary_id = 'TARGET'
        vocab2.vocabulary_name = 'Standard vocabulary'
        vocab2.vocabulary_reference = 'vocab ref'
        vocab2.vocabulary_concept_id = 0

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
        concept.concept_name = 'ARTIFICIAL_CONCEPT'
        concept.domain_id = domain.domain_id
        concept.vocabulary_id = vocab1.vocabulary_id
        concept.concept_class_id = conc_class.concept_class_id
        concept.concept_code = 'Artificial Concept'
        concept.valid_start_date = '1970-01-01'
        concept.valid_end_date = '2099-12-31'

        concept1 = cdm600.Concept()
        concept1.concept_id = 1
        concept1.concept_name = 'Non-standard concept with no mapping to standard concept'
        concept1.domain_id = domain.domain_id
        concept1.vocabulary_id = vocab1.vocabulary_id
        concept1.concept_class_id = conc_class.concept_class_id
        concept1.concept_code = 'SOURCE_1'
        concept1.valid_start_date = '1970-01-01'
        concept1.valid_end_date = '2099-12-31'

        concept2 = cdm600.Concept()
        concept2.concept_id = 2
        concept2.concept_name = 'Non-standard concept with 1 mapping to standard concept'
        concept2.domain_id = domain.domain_id
        concept2.vocabulary_id = vocab1.vocabulary_id
        concept2.concept_class_id = conc_class.concept_class_id
        concept2.concept_code = 'SOURCE_2'
        concept2.valid_start_date = '1970-01-01'
        concept2.valid_end_date = '2099-12-31'

        concept3 = cdm600.Concept()
        concept3.concept_id = 3
        concept3.concept_name = 'Non-standard concept with multiple mappings to standard concept'
        concept3.domain_id = domain.domain_id
        concept3.vocabulary_id = vocab1.vocabulary_id
        concept3.concept_class_id = conc_class.concept_class_id
        concept3.concept_code = 'SOURCE_3'
        concept3.valid_start_date = '1970-01-01'
        concept3.valid_end_date = '2099-12-31'

        concept4 = cdm600.Concept()
        concept4.concept_id = 4
        concept4.concept_name = 'Standard concept 1'
        concept4.domain_id = domain.domain_id
        concept4.vocabulary_id = vocab2.vocabulary_id
        concept4.concept_class_id = conc_class.concept_class_id
        concept4.concept_code = 'TARGET_1'
        concept4.standard_concept = 'S'
        concept4.valid_start_date = '1970-01-01'
        concept4.valid_end_date = '2099-12-31'

        concept5 = cdm600.Concept()
        concept5.concept_id = 5
        concept5.concept_name = 'Standard concept 2'
        concept5.domain_id = domain.domain_id
        concept5.vocabulary_id = vocab2.vocabulary_id
        concept5.concept_class_id = conc_class.concept_class_id
        concept5.concept_code = 'TARGET_2'
        concept5.standard_concept = 'S'
        concept5.valid_start_date = '1970-01-01'
        concept5.valid_end_date = '2099-12-31'

        relationship1 = cdm600.Relationship()
        relationship1.relationship_id = 'Maps to'
        relationship1.relationship_name = 'Non-standard to Standard map (OMOP)'
        relationship1.is_hierarchical = '0'
        relationship1.defines_ancestry = '0'
        relationship1.reverse_relationship_id = 'Mapped from'
        relationship1.relationship_concept_id = 0

        relationship2 = cdm600.Relationship()
        relationship2.relationship_id = 'Mapped from'
        relationship2.relationship_name = 'Standard to Non-standard map (OMOP)'
        relationship2.is_hierarchical = '0'
        relationship2.defines_ancestry = '0'
        relationship2.reverse_relationship_id = 'Maps to'
        relationship2.relationship_concept_id = 0

        con_relationship1 = cdm600.ConceptRelationship()
        con_relationship1.concept_id_1 = 2
        con_relationship1.concept_id_2 = 4
        con_relationship1.relationship_id = 'Maps to'
        con_relationship1.valid_start_date = '1970-01-01'
        con_relationship1.valid_end_date = '2099-12-31'
        con_relationship1.invalid_reason = None

        con_relationship2 = cdm600.ConceptRelationship()
        con_relationship2.concept_id_1 = 3
        con_relationship2.concept_id_2 = 4
        con_relationship2.relationship_id = 'Maps to'
        con_relationship2.valid_start_date = '1970-01-01'
        con_relationship2.valid_end_date = '2099-12-31'
        con_relationship2.invalid_reason = None

        con_relationship3 = cdm600.ConceptRelationship()
        con_relationship3.concept_id_1 = 3
        con_relationship3.concept_id_2 = 5
        con_relationship3.relationship_id = 'Maps to'
        con_relationship3.valid_start_date = '1970-01-01'
        con_relationship3.valid_end_date = '2099-12-31'
        con_relationship3.invalid_reason = None

        session.add_all([vocab1, vocab2, conc_class, domain, relationship1, relationship2,
                         concept, concept1, concept2, concept3, concept4, concept5,
                         con_relationship1, con_relationship2, con_relationship3])
