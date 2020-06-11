# Copyright 2019 The Hyve
#
# Licensed under the GNU General vocab License, version 3,
# or (at your option) any later version (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.gnu.org/licenses/
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General vocab License for more details.
#
# OMOP CDM v6, with oncology extension, defined by https://github.com/OHDSI/CommonDataModel/tree/Dev/PostgreSQL #30d851a
# coding: utf-8

from sqlalchemy import Column, Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import relationship

from .._defaults import VOCAB_SCHEMA
from ... import Base


class Concept(Base):
    __tablename__ = 'concept'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    concept_id = Column(Integer, primary_key=True, unique=True)
    concept_name = Column(String(255), nullable=False)
    domain_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.domain.domain_id'), nullable=False, index=True)
    vocabulary_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.vocabulary.vocabulary_id'), nullable=False, index=True)
    concept_class_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept_class.concept_class_id'), nullable=False, index=True)
    standard_concept = Column(String(1))
    concept_code = Column(String(50), nullable=False, index=True)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1))

    concept_class = relationship('ConceptClass', primaryjoin='Concept.concept_class_id == ConceptClass.concept_class_id')
    domain = relationship('Domain', primaryjoin='Concept.domain_id == Domain.domain_id')
    vocabulary = relationship('Vocabulary', primaryjoin='Concept.vocabulary_id == Vocabulary.vocabulary_id')


class ConceptAncestor(Base):
    __tablename__ = 'concept_ancestor'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    ancestor_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)
    descendant_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)
    min_levels_of_separation = Column(Integer, nullable=False)
    max_levels_of_separation = Column(Integer, nullable=False)

    ancestor_concept = relationship('Concept', primaryjoin='ConceptAncestor.ancestor_concept_id == Concept.concept_id')
    descendant_concept = relationship('Concept', primaryjoin='ConceptAncestor.descendant_concept_id == Concept.concept_id')


class ConceptClass(Base):
    __tablename__ = 'concept_class'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    concept_class_id = Column(String(20), primary_key=True, unique=True)
    concept_class_name = Column(String(255), nullable=False)
    concept_class_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    concept_class_concept = relationship('Concept', primaryjoin='ConceptClass.concept_class_concept_id == Concept.concept_id')


class ConceptRelationship(Base):
    __tablename__ = 'concept_relationship'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    concept_id_1 = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)
    concept_id_2 = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)
    relationship_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.relationship.relationship_id'), primary_key=True, nullable=False, index=True)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1))

    concept1 = relationship('Concept', primaryjoin='ConceptRelationship.concept_id_1 == Concept.concept_id')
    concept2 = relationship('Concept', primaryjoin='ConceptRelationship.concept_id_2 == Concept.concept_id')
    relationship = relationship('Relationship')


class ConceptSynonym(Base):
    __tablename__ = 'concept_synonym'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)
    concept_synonym_name = Column(String(1000), primary_key=True, nullable=False)
    language_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False)

    concept = relationship('Concept', primaryjoin='ConceptSynonym.concept_id == Concept.concept_id')
    language_concept = relationship('Concept', primaryjoin='ConceptSynonym.language_concept_id == Concept.concept_id')


class Domain(Base):
    __tablename__ = 'domain'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    domain_id = Column(String(20), primary_key=True, unique=True)
    domain_name = Column(String(255), nullable=False)
    domain_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    domain_concept = relationship('Concept',
                                  primaryjoin='Domain.domain_concept_id == Concept.concept_id',
                                  post_update=True)


class DrugStrength(Base):
    __tablename__ = 'drug_strength'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    drug_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)
    ingredient_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)
    amount_value = Column(Numeric)
    amount_unit_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    numerator_value = Column(Numeric)
    numerator_unit_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    denominator_value = Column(Numeric)
    denominator_unit_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    box_size = Column(Integer)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1))

    amount_unit_concept = relationship('Concept', primaryjoin='DrugStrength.amount_unit_concept_id == Concept.concept_id')
    denominator_unit_concept = relationship('Concept', primaryjoin='DrugStrength.denominator_unit_concept_id == Concept.concept_id')
    drug_concept = relationship('Concept', primaryjoin='DrugStrength.drug_concept_id == Concept.concept_id')
    ingredient_concept = relationship('Concept', primaryjoin='DrugStrength.ingredient_concept_id == Concept.concept_id')
    numerator_unit_concept = relationship('Concept', primaryjoin='DrugStrength.numerator_unit_concept_id == Concept.concept_id')


class Relationship(Base):
    __tablename__ = 'relationship'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    relationship_id = Column(String(20), primary_key=True, unique=True)
    relationship_name = Column(String(255), nullable=False)
    is_hierarchical = Column(String(1), nullable=False)
    defines_ancestry = Column(String(1), nullable=False)
    reverse_relationship_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.relationship.relationship_id'), nullable=False)
    relationship_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    relationship_concept = relationship('Concept')
    reverse_relationship = relationship('Relationship', remote_side=[relationship_id])


class SourceToConceptMap(Base):
    __tablename__ = 'source_to_concept_map'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    source_code = Column(Text, primary_key=True, nullable=False, index=True)
    source_concept_id = Column(Integer, nullable=False)
    source_vocabulary_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.vocabulary.vocabulary_id'), primary_key=True, nullable=False, index=True)
    source_code_description = Column(String(255))
    target_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False, index=True)
    target_vocabulary_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.vocabulary.vocabulary_id'), nullable=False, index=True)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, primary_key=True, nullable=False)
    invalid_reason = Column(String(1))

    source_vocabulary = relationship('Vocabulary', primaryjoin='SourceToConceptMap.source_vocabulary_id == Vocabulary.vocabulary_id')
    target_concept = relationship('Concept')
    target_vocabulary = relationship('Vocabulary', primaryjoin='SourceToConceptMap.target_vocabulary_id == Vocabulary.vocabulary_id')


class Vocabulary(Base):
    __tablename__ = 'vocabulary'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    vocabulary_id = Column(String(20), primary_key=True, unique=True)
    vocabulary_name = Column(String(255), nullable=False)
    vocabulary_reference = Column(String(255), nullable=False)
    vocabulary_version = Column(String(255))
    vocabulary_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    vocabulary_concept = relationship('Concept',
                                      primaryjoin='Vocabulary.vocabulary_concept_id == Concept.concept_id',
                                      post_update=True)
