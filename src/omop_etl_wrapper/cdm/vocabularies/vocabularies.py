from sqlalchemy import Column, Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declared_attr

from .._schema_placeholders import VOCAB_SCHEMA


class BaseConcept:
    __tablename__ = 'concept'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    concept_id = Column(Integer, primary_key=True, unique=True)
    concept_name = Column(String(255), nullable=False)

    @declared_attr
    def domain_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.domain.domain_id'), nullable=False, index=True)

    @declared_attr
    def vocabulary_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.vocabulary.vocabulary_id'), nullable=False, index=True)

    @declared_attr
    def concept_class_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept_class.concept_class_id'), nullable=False, index=True)

    standard_concept = Column(String(1))
    concept_code = Column(String(50), nullable=False, index=True)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1))

    @declared_attr
    def concept_class(cls):
        return relationship('ConceptClass', primaryjoin='Concept.concept_class_id == ConceptClass.concept_class_id')

    @declared_attr
    def domain(cls):
        return relationship('Domain', primaryjoin='Concept.domain_id == Domain.domain_id')

    @declared_attr
    def vocabulary(cls):
        return relationship('Vocabulary', primaryjoin='Concept.vocabulary_id == Vocabulary.vocabulary_id')


class BaseConceptAncestor:
    __tablename__ = 'concept_ancestor'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    @declared_attr
    def ancestor_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)

    @declared_attr
    def descendant_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)

    min_levels_of_separation = Column(Integer, nullable=False)
    max_levels_of_separation = Column(Integer, nullable=False)

    @declared_attr
    def ancestor_concept(cls):
        return relationship('Concept', primaryjoin='ConceptAncestor.ancestor_concept_id == Concept.concept_id')

    @declared_attr
    def descendant_concept(cls):
        return relationship('Concept', primaryjoin='ConceptAncestor.descendant_concept_id == Concept.concept_id')


class BaseConceptClass:
    __tablename__ = 'concept_class'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    concept_class_id = Column(String(20), primary_key=True, unique=True)
    concept_class_name = Column(String(255), nullable=False)

    @declared_attr
    def concept_class_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def concept_class_concept(cls):
        return relationship('Concept', primaryjoin='ConceptClass.concept_class_concept_id == Concept.concept_id')


class BaseConceptRelationship:
    __tablename__ = 'concept_relationship'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    @declared_attr
    def concept_id_1(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)

    @declared_attr
    def concept_id_2(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)

    @declared_attr
    def relationship_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.relationship.relationship_id'), primary_key=True, nullable=False, index=True)

    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1))

    @declared_attr
    def concept1(cls):
        return relationship('Concept', primaryjoin='ConceptRelationship.concept_id_1 == Concept.concept_id')

    @declared_attr
    def concept2(cls):
        return relationship('Concept', primaryjoin='ConceptRelationship.concept_id_2 == Concept.concept_id')

    @declared_attr
    def relationship(cls):
        return relationship('Relationship')


class BaseConceptSynonym:
    __tablename__ = 'concept_synonym'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    @declared_attr
    def concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)

    concept_synonym_name = Column(String(1000), primary_key=True, nullable=False)

    @declared_attr
    def language_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False)

    @declared_attr
    def concept(cls):
        return relationship('Concept', primaryjoin='ConceptSynonym.concept_id == Concept.concept_id')

    @declared_attr
    def language_concept(cls):
        return relationship('Concept', primaryjoin='ConceptSynonym.language_concept_id == Concept.concept_id')


class BaseDomain:
    __tablename__ = 'domain'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    domain_id = Column(String(20), primary_key=True, unique=True)
    domain_name = Column(String(255), nullable=False)

    @declared_attr
    def domain_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def domain_concept(cls):
        return relationship('Concept', primaryjoin='Domain.domain_concept_id == Concept.concept_id', post_update=True)


class BaseDrugStrength:
    __tablename__ = 'drug_strength'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    @declared_attr
    def drug_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)

    @declared_attr
    def ingredient_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)

    amount_value = Column(Numeric)

    @declared_attr
    def amount_unit_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    numerator_value = Column(Numeric)

    @declared_attr
    def numerator_unit_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    denominator_value = Column(Numeric)

    @declared_attr
    def denominator_unit_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    box_size = Column(Integer)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1))

    @declared_attr
    def amount_unit_concept(cls):
        return relationship('Concept', primaryjoin='DrugStrength.amount_unit_concept_id == Concept.concept_id')

    @declared_attr
    def denominator_unit_concept(cls):
        return relationship('Concept', primaryjoin='DrugStrength.denominator_unit_concept_id == Concept.concept_id')

    @declared_attr
    def drug_concept(cls):
        return relationship('Concept', primaryjoin='DrugStrength.drug_concept_id == Concept.concept_id')

    @declared_attr
    def ingredient_concept(cls):
        return relationship('Concept', primaryjoin='DrugStrength.ingredient_concept_id == Concept.concept_id')

    @declared_attr
    def numerator_unit_concept(cls):
        return relationship('Concept', primaryjoin='DrugStrength.numerator_unit_concept_id == Concept.concept_id')


class BaseRelationship:
    __tablename__ = 'relationship'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    relationship_id = Column(String(20), primary_key=True, unique=True)
    relationship_name = Column(String(255), nullable=False)
    is_hierarchical = Column(String(1), nullable=False)
    defines_ancestry = Column(String(1), nullable=False)

    @declared_attr
    def reverse_relationship_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.relationship.relationship_id'), nullable=False)

    @declared_attr
    def relationship_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def relationship_concept(cls):
        return relationship('Concept')

    @declared_attr
    def reverse_relationship(cls):
        return relationship('Relationship', remote_side=[cls.relationship_id])


class BaseSourceToConceptMap:
    __tablename__ = 'source_to_concept_map'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    source_code = Column(Text, primary_key=True, nullable=False, index=True)
    source_concept_id = Column(Integer, nullable=False)

    @declared_attr
    def source_vocabulary_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.vocabulary.vocabulary_id'), primary_key=True, nullable=False, index=True)

    source_code_description = Column(String(255))

    @declared_attr
    def target_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)

    @declared_attr
    def target_vocabulary_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.vocabulary.vocabulary_id'), nullable=False, index=True)

    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, primary_key=True, nullable=False)
    invalid_reason = Column(String(1))

    @declared_attr
    def source_vocabulary(cls):
        return relationship('Vocabulary', primaryjoin='SourceToConceptMap.source_vocabulary_id == Vocabulary.vocabulary_id')

    @declared_attr
    def target_concept(cls):
        return relationship('Concept')

    @declared_attr
    def target_vocabulary(cls):
        return relationship('Vocabulary', primaryjoin='SourceToConceptMap.target_vocabulary_id == Vocabulary.vocabulary_id')


class BaseVocabulary:
    __tablename__ = 'vocabulary'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    vocabulary_id = Column(String(20), primary_key=True, unique=True)
    vocabulary_name = Column(String(255), nullable=False)
    vocabulary_reference = Column(String(255), nullable=False)
    vocabulary_version = Column(String(255))

    @declared_attr
    def vocabulary_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def vocabulary_concept(cls):
        return relationship('Concept', primaryjoin='Vocabulary.vocabulary_concept_id == Concept.concept_id', post_update=True)


class BaseCohortDefinition:
    __tablename__ = 'cohort_definition'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    cohort_definition_id = Column(Integer, primary_key=True, index=True)
    cohort_definition_name = Column(String(255), nullable=False)
    cohort_definition_description = Column(Text)

    @declared_attr
    def definition_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    cohort_definition_syntax = Column(Text)

    @declared_attr
    def subject_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    cohort_initiation_date = Column(Date)

    @declared_attr
    def definition_type_concept(cls):
        return relationship('Concept', primaryjoin='CohortDefinition.definition_type_concept_id == Concept.concept_id')

    @declared_attr
    def subject_concept(cls):
        return relationship('Concept', primaryjoin='CohortDefinition.subject_concept_id == Concept.concept_id')
