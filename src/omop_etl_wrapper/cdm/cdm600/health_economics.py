from sqlalchemy import (BigInteger, Column, Date, ForeignKey, Integer,
                        Numeric, String)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

from .._schema_placeholders import VOCAB_SCHEMA, CDM_SCHEMA


class BaseCostCdm600:
    __tablename__ = 'cost'
    __table_args__ = {'schema': CDM_SCHEMA}

    cost_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    cost_event_id = Column(BigInteger, nullable=False)
    cost_event_field_concept_id = Column(Integer, nullable=False)

    @declared_attr
    def cost_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def cost_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def currency_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    cost = Column(Numeric)
    incurred_date = Column(Date, nullable=False)
    billed_date = Column(Date)
    paid_date = Column(Date)

    @declared_attr
    def revenue_code_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def drg_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    cost_source_value = Column(String(50))

    @declared_attr
    def cost_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    revenue_code_source_value = Column(String(50))
    drg_source_value = Column(String(3))

    @declared_attr
    def payer_plan_period_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.payer_plan_period.payer_plan_period_id'))

    @declared_attr
    def cost_concept(cls):
        return relationship('Concept', primaryjoin='Cost.cost_concept_id == Concept.concept_id')

    @declared_attr
    def cost_source_concept(cls):
        return relationship('Concept', primaryjoin='Cost.cost_source_concept_id == Concept.concept_id')

    @declared_attr
    def cost_type_concept(cls):
        return relationship('Concept', primaryjoin='Cost.cost_type_concept_id == Concept.concept_id')

    @declared_attr
    def currency_concept(cls):
        return relationship('Concept', primaryjoin='Cost.currency_concept_id == Concept.concept_id')

    @declared_attr
    def drg_concept(cls):
        return relationship('Concept', primaryjoin='Cost.drg_concept_id == Concept.concept_id')

    @declared_attr
    def payer_plan_period(cls):
        return relationship('PayerPlanPeriod')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def revenue_code_concept(cls):
        return relationship('Concept', primaryjoin='Cost.revenue_code_concept_id == Concept.concept_id')


class BasePayerPlanPeriodCdm600:
    __tablename__ = 'payer_plan_period'
    __table_args__ = {'schema': CDM_SCHEMA}

    payer_plan_period_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def contract_person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'))

    payer_plan_period_start_date = Column(Date, nullable=False)
    payer_plan_period_end_date = Column(Date, nullable=False)

    @declared_attr
    def payer_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def plan_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def contract_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def sponsor_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def stop_reason_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    payer_source_value = Column(String(50))

    @declared_attr
    def payer_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    plan_source_value = Column(String(50))

    @declared_attr
    def plan_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    contract_source_value = Column(String(50))

    @declared_attr
    def contract_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    sponsor_source_value = Column(String(50))

    @declared_attr
    def sponsor_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    family_source_value = Column(String(50))
    stop_reason_source_value = Column(String(50))

    @declared_attr
    def stop_reason_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def contract_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.contract_concept_id == Concept.concept_id')

    @declared_attr
    def contract_person(cls):
        return relationship('Person', primaryjoin='PayerPlanPeriod.contract_person_id == Person.person_id')

    @declared_attr
    def contract_source_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.contract_source_concept_id == Concept.concept_id')

    @declared_attr
    def payer_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.payer_concept_id == Concept.concept_id')

    @declared_attr
    def payer_source_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.payer_source_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person', primaryjoin='PayerPlanPeriod.person_id == Person.person_id')

    @declared_attr
    def plan_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.plan_concept_id == Concept.concept_id')

    @declared_attr
    def plan_source_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.plan_source_concept_id == Concept.concept_id')

    @declared_attr
    def sponsor_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.sponsor_concept_id == Concept.concept_id')

    @declared_attr
    def sponsor_source_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.sponsor_source_concept_id == Concept.concept_id')

    @declared_attr
    def stop_reason_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.stop_reason_concept_id == Concept.concept_id')

    @declared_attr
    def stop_reason_source_concept(cls):
        return relationship('Concept', primaryjoin='PayerPlanPeriod.stop_reason_source_concept_id == Concept.concept_id')
