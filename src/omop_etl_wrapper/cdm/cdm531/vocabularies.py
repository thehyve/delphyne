from sqlalchemy import Column, Date, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from .._schema_placeholders import VOCAB_SCHEMA
from ... import Base


class CohortDefinition(Base):
    __tablename__ = 'cohort_definition'
    __table_args__ = {'schema': VOCAB_SCHEMA}

    cohort_definition_id = Column(Integer, primary_key=True, index=True)
    cohort_definition_name = Column(String(255), nullable=False)
    cohort_definition_description = Column(Text)
    definition_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    cohort_definition_syntax = Column(Text)
    subject_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    cohort_initiation_date = Column(Date)

    definition_type_concept = relationship('Concept', primaryjoin='CohortDefinition.definition_type_concept_id == Concept.concept_id')
    subject_concept = relationship('Concept', primaryjoin='CohortDefinition.subject_concept_id == Concept.concept_id')
