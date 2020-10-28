from sqlalchemy import (BigInteger, Column, DateTime, ForeignKey,
                        Integer, Numeric)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

from .._schema_placeholders import VOCAB_SCHEMA, CDM_SCHEMA


class BaseConditionEraCdm600:
    __tablename__ = 'condition_era'
    __table_args__ = {'schema': CDM_SCHEMA}

    condition_era_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def condition_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    condition_era_start_datetime = Column(DateTime, nullable=False)
    condition_era_end_datetime = Column(DateTime, nullable=False)
    condition_occurrence_count = Column(Integer)

    @declared_attr
    def condition_concept(cls):
        return relationship('Concept')

    @declared_attr
    def person(cls):
        return relationship('Person')


class BaseDoseEraCdm600:
    __tablename__ = 'dose_era'
    __table_args__ = {'schema': CDM_SCHEMA}

    dose_era_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def drug_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    @declared_attr
    def unit_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    dose_value = Column(Numeric, nullable=False)
    dose_era_start_datetime = Column(DateTime, nullable=False)
    dose_era_end_datetime = Column(DateTime, nullable=False)

    @declared_attr
    def drug_concept(cls):
        return relationship('Concept', primaryjoin='DoseEra.drug_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def unit_concept(cls):
        return relationship('Concept', primaryjoin='DoseEra.unit_concept_id == Concept.concept_id')


class BaseDrugEraCdm600:
    __tablename__ = 'drug_era'
    __table_args__ = {'schema': CDM_SCHEMA}

    drug_era_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def drug_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    drug_era_start_datetime = Column(DateTime, nullable=False)
    drug_era_end_datetime = Column(DateTime, nullable=False)
    drug_exposure_count = Column(Integer)
    gap_days = Column(Integer)

    @declared_attr
    def drug_concept(cls):
        return relationship('Concept')

    @declared_attr
    def person(cls):
        return relationship('Person')
