# Copyright 2019 The Hyve
#
# Licensed under the GNU General Public License, version 3,
# or (at your option) any later version (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.gnu.org/licenses/
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# OMOP CDM v6, with oncology extension, defined by https://github.com/OHDSI/CommonDataModel/tree/Dev/PostgreSQL #30d851a
# coding: utf-8

from sqlalchemy import BigInteger, Column, Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship

from .._defaults import VOCAB_SCHEMA, CDM_SCHEMA
from ... import Base


class Cost(Base):
    __tablename__ = 'cost'
    __table_args__ = {'schema': CDM_SCHEMA}

    cost_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    cost_event_id = Column(BigInteger, nullable=False)
    cost_event_field_concept_id = Column(Integer, nullable=False)
    cost_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    cost_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    currency_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    cost = Column(Numeric)
    incurred_date = Column(Date, nullable=False)
    billed_date = Column(Date)
    paid_date = Column(Date)
    revenue_code_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    drg_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    cost_source_value = Column(String(50))
    cost_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    revenue_code_source_value = Column(String(50))
    drg_source_value = Column(String(3))
    payer_plan_period_id = Column(ForeignKey(f'{CDM_SCHEMA}.payer_plan_period.payer_plan_period_id'))

    cost_concept = relationship('Concept', primaryjoin='Cost.cost_concept_id == Concept.concept_id')
    cost_source_concept = relationship('Concept', primaryjoin='Cost.cost_source_concept_id == Concept.concept_id')
    cost_type_concept = relationship('Concept', primaryjoin='Cost.cost_type_concept_id == Concept.concept_id')
    currency_concept = relationship('Concept', primaryjoin='Cost.currency_concept_id == Concept.concept_id')
    drg_concept = relationship('Concept', primaryjoin='Cost.drg_concept_id == Concept.concept_id')
    payer_plan_period = relationship('PayerPlanPeriod')
    person = relationship('Person')
    revenue_code_concept = relationship('Concept', primaryjoin='Cost.revenue_code_concept_id == Concept.concept_id')


class PayerPlanPeriod(Base):
    __tablename__ = 'payer_plan_period'
    __table_args__ = {'schema': CDM_SCHEMA}

    payer_plan_period_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    contract_person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'))
    payer_plan_period_start_date = Column(Date, nullable=False)
    payer_plan_period_end_date = Column(Date, nullable=False)
    payer_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    plan_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    contract_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    sponsor_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    stop_reason_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    payer_source_value = Column(String(50))
    payer_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    plan_source_value = Column(String(50))
    plan_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    contract_source_value = Column(String(50))
    contract_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    sponsor_source_value = Column(String(50))
    sponsor_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    family_source_value = Column(String(50))
    stop_reason_source_value = Column(String(50))
    stop_reason_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    contract_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.contract_concept_id == Concept.concept_id')
    contract_person = relationship('Person', primaryjoin='PayerPlanPeriod.contract_person_id == Person.person_id')
    contract_source_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.contract_source_concept_id == Concept.concept_id')
    payer_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.payer_concept_id == Concept.concept_id')
    payer_source_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.payer_source_concept_id == Concept.concept_id')
    person = relationship('Person', primaryjoin='PayerPlanPeriod.person_id == Person.person_id')
    plan_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.plan_concept_id == Concept.concept_id')
    plan_source_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.plan_source_concept_id == Concept.concept_id')
    sponsor_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.sponsor_concept_id == Concept.concept_id')
    sponsor_source_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.sponsor_source_concept_id == Concept.concept_id')
    stop_reason_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.stop_reason_concept_id == Concept.concept_id')
    stop_reason_source_concept = relationship('Concept', primaryjoin='PayerPlanPeriod.stop_reason_source_concept_id == Concept.concept_id')

