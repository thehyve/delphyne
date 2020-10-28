from sqlalchemy import (BigInteger, Column, DateTime, ForeignKey,
                        Integer, String, Date, Numeric)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship


class BaseMeasurementOncExt:
    __tablename__ = 'measurement'
    __table_args__ = {'schema': 'cdm_schema'}

    measurement_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'cdm_schema.person.person_id'), nullable=False, index=True)

    @declared_attr
    def measurement_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'), nullable=False, index=True)

    measurement_date = Column(Date)
    measurement_datetime = Column(DateTime, nullable=False)
    measurement_time = Column(String(10))

    @declared_attr
    def measurement_type_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'), nullable=False)

    @declared_attr
    def operator_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'))

    value_as_number = Column(Numeric)

    @declared_attr
    def value_as_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'))

    @declared_attr
    def unit_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'))

    range_low = Column(Numeric)
    range_high = Column(Numeric)

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'cdm_schema.provider.provider_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'cdm_schema.visit_occurrence.visit_occurrence_id'), index=True)

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'cdm_schema.visit_detail.visit_detail_id'))

    measurement_source_value = Column(String(50))

    @declared_attr
    def measurement_source_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'), nullable=False)

    unit_source_value = Column(String(50))
    value_source_value = Column(String(50))
    modifier_of_event_id = Column(BigInteger)

    @declared_attr
    def modifier_of_field_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'))

    @declared_attr
    def measurement_concept(cls):
        return relationship('Concept',
                            primaryjoin='Measurement.measurement_concept_id == Concept.concept_id')

    @declared_attr
    def measurement_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='Measurement.measurement_source_concept_id == Concept.concept_id')

    @declared_attr
    def measurement_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='Measurement.measurement_type_concept_id == Concept.concept_id')

    @declared_attr
    def modifier_of_field_concept(cls):
        return relationship('Concept',
                            primaryjoin='Measurement.modifier_of_field_concept_id == Concept.concept_id')

    @declared_attr
    def operator_concept(cls):
        return relationship('Concept',
                            primaryjoin='Measurement.operator_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def unit_concept(cls):
        return relationship('Concept',
                            primaryjoin='Measurement.unit_concept_id == Concept.concept_id')

    @declared_attr
    def value_as_concept(cls):
        return relationship('Concept',
                            primaryjoin='Measurement.value_as_concept_id == Concept.concept_id')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')


class BaseEpisodeOncExt:
    __tablename__ = 'episode'
    __table_args__ = {'schema': 'cdm_schema'}

    episode_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'cdm_schema.person.person_id'), nullable=False, index=True)

    episode_start_datetime = Column(DateTime, nullable=False)
    episode_end_datetime = Column(DateTime, nullable=False)

    @declared_attr
    def episode_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'), nullable=False, index=True)

    @declared_attr
    def episode_parent_id(cls):
        return Column(ForeignKey(f'cdm_schema.episode.episode_id'))

    episode_number = Column(Integer)

    @declared_attr
    def episode_object_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'), nullable=False)

    @declared_attr
    def episode_type_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'), nullable=False)

    episode_source_value = Column(String(50))

    @declared_attr
    def episode_source_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'))


    @declared_attr
    def episode_concept(cls):
        return relationship('Concept',
                            primaryjoin='Episode.episode_concept_id == Concept.concept_id')

    @declared_attr
    def episode_object_concept(cls):
        return relationship('Concept',
                            primaryjoin='Episode.episode_object_concept_id == Concept.concept_id')

    @declared_attr
    def episode_parent(cls):
        return relationship('Episode', remote_side=[cls.episode_id])

    @declared_attr
    def episode_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='Episode.episode_source_concept_id == Concept.concept_id')

    @declared_attr
    def episode_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='Episode.episode_type_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')


class BaseEpisodeEventOncExt:
    __tablename__ = 'episode_event'
    __table_args__ = {'schema': 'cdm_schema'}

    episode_id = Column(BigInteger, primary_key=True, nullable=False, index=True)
    event_id = Column(BigInteger, primary_key=True, nullable=False)

    @declared_attr
    def event_field_concept_id(cls):
        return Column(ForeignKey(f'vocabulary_schema.concept.concept_id'),
                      primary_key=True, nullable=False, index=True)

    @declared_attr
    def event_field_concept(cls):
        return relationship('Concept')
