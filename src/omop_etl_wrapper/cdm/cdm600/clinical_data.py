from sqlalchemy import BigInteger, Column, Date, DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import relationship

from .._schema_placeholders import VOCAB_SCHEMA, CDM_SCHEMA
from ... import Base


class Measurement(Base):
    __tablename__ = 'measurement'
    __table_args__ = {'schema': CDM_SCHEMA}

    measurement_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    measurement_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    measurement_date = Column(Date)
    measurement_datetime = Column(DateTime, nullable=False)
    measurement_time = Column(String(10))
    measurement_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    operator_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    value_as_number = Column(Numeric)
    value_as_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    unit_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    range_low = Column(Numeric)
    range_high = Column(Numeric)
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)
    visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    measurement_source_value = Column(String(50))
    measurement_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    unit_source_value = Column(String(50))
    value_source_value = Column(String(50))

    measurement_concept = relationship('Concept',
                                       primaryjoin='Measurement.measurement_concept_id == Concept.concept_id')
    measurement_source_concept = relationship('Concept',
                                              primaryjoin='Measurement.measurement_source_concept_id == Concept.concept_id')
    measurement_type_concept = relationship('Concept',
                                            primaryjoin='Measurement.measurement_type_concept_id == Concept.concept_id')
    operator_concept = relationship('Concept', primaryjoin='Measurement.operator_concept_id == Concept.concept_id')
    person = relationship('Person')
    provider = relationship('Provider')
    unit_concept = relationship('Concept', primaryjoin='Measurement.unit_concept_id == Concept.concept_id')
    value_as_concept = relationship('Concept', primaryjoin='Measurement.value_as_concept_id == Concept.concept_id')
    visit_detail = relationship('VisitDetail')
    visit_occurrence = relationship('VisitOccurrence')
