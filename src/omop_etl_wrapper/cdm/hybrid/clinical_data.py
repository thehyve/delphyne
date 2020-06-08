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
from .._defaults import VOCAB_SCHEMA, CDM_SCHEMA
from sqlalchemy import BigInteger, Column, Date, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import relationship

from omop_etl_wrapper import Base


class ConditionOccurrence(Base):
    __tablename__ = 'condition_occurrence'
    __table_args__ = {'schema': CDM_SCHEMA}

    condition_occurrence_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    condition_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    condition_start_date = Column(Date)
    condition_start_datetime = Column(DateTime, nullable=False)
    condition_end_date = Column(Date)
    condition_end_datetime = Column(DateTime)
    condition_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    stop_reason = Column(String(20))
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)
    visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    condition_source_value = Column(String(50))
    condition_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    condition_status_source_value = Column(String(50))
    condition_status_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    condition_concept = relationship('Concept',
                                     primaryjoin='ConditionOccurrence.condition_concept_id == Concept.concept_id')
    condition_source_concept = relationship('Concept',
                                            primaryjoin='ConditionOccurrence.condition_source_concept_id == Concept.concept_id')
    condition_status_concept = relationship('Concept',
                                            primaryjoin='ConditionOccurrence.condition_status_concept_id == Concept.concept_id')
    condition_type_concept = relationship('Concept',
                                          primaryjoin='ConditionOccurrence.condition_type_concept_id == Concept.concept_id')
    person = relationship('Person')
    provider = relationship('Provider')
    visit_detail = relationship('VisitDetail')
    visit_occurrence = relationship('VisitOccurrence')


class DeviceExposure(Base):
    __tablename__ = 'device_exposure'
    __table_args__ = {'schema': CDM_SCHEMA}

    device_exposure_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    device_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    device_exposure_start_date = Column(Date)
    device_exposure_start_datetime = Column(DateTime, nullable=False)
    device_exposure_end_date = Column(Date)
    device_exposure_end_datetime = Column(DateTime)
    device_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    unique_device_id = Column(String(50))
    quantity = Column(Integer)
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)
    visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    device_source_value = Column(String(100))
    device_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    device_concept = relationship('Concept', primaryjoin='DeviceExposure.device_concept_id == Concept.concept_id')
    device_source_concept = relationship('Concept',
                                         primaryjoin='DeviceExposure.device_source_concept_id == Concept.concept_id')
    device_type_concept = relationship('Concept',
                                       primaryjoin='DeviceExposure.device_type_concept_id == Concept.concept_id')
    person = relationship('Person')
    provider = relationship('Provider')
    visit_detail = relationship('VisitDetail')
    visit_occurrence = relationship('VisitOccurrence')


class DrugExposure(Base):
    __tablename__ = 'drug_exposure'
    __table_args__ = {'schema': CDM_SCHEMA}

    drug_exposure_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    drug_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    drug_exposure_start_date = Column(Date)
    drug_exposure_start_datetime = Column(DateTime, nullable=False)
    drug_exposure_end_date = Column(Date)
    drug_exposure_end_datetime = Column(DateTime, nullable=False)
    verbatim_end_date = Column(Date)
    drug_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    stop_reason = Column(String(20))
    refills = Column(Integer)
    quantity = Column(Numeric)
    days_supply = Column(Integer)
    sig = Column(Text)
    route_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    lot_number = Column(String(50))
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)
    visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    drug_source_value = Column(String(50))
    drug_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    route_source_value = Column(String(50))
    dose_unit_source_value = Column(String(50))

    drug_concept = relationship('Concept', primaryjoin='DrugExposure.drug_concept_id == Concept.concept_id')
    drug_source_concept = relationship('Concept',
                                       primaryjoin='DrugExposure.drug_source_concept_id == Concept.concept_id')
    drug_type_concept = relationship('Concept', primaryjoin='DrugExposure.drug_type_concept_id == Concept.concept_id')
    person = relationship('Person')
    provider = relationship('Provider')
    route_concept = relationship('Concept', primaryjoin='DrugExposure.route_concept_id == Concept.concept_id')
    visit_detail = relationship('VisitDetail')
    visit_occurrence = relationship('VisitOccurrence')


class FactRelationship(Base):
    __tablename__ = 'fact_relationship'
    __table_args__ = {'schema': CDM_SCHEMA}

    fact_relationship_id = Column(Integer, primary_key=True)
    domain_concept_id_1 = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    fact_id_1 = Column(Integer, nullable=False)
    domain_concept_id_2 = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    fact_id_2 = Column(Integer, nullable=False)
    relationship_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)


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
    modifier_of_event_id = Column(BigInteger)
    modifier_of_field_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    measurement_concept = relationship('Concept',
                                       primaryjoin='Measurement.measurement_concept_id == Concept.concept_id')
    measurement_source_concept = relationship('Concept',
                                              primaryjoin='Measurement.measurement_source_concept_id == Concept.concept_id')
    measurement_type_concept = relationship('Concept',
                                            primaryjoin='Measurement.measurement_type_concept_id == Concept.concept_id')
    modifier_of_field_concept = relationship('Concept',
                                             primaryjoin='Measurement.modifier_of_field_concept_id == Concept.concept_id')
    operator_concept = relationship('Concept', primaryjoin='Measurement.operator_concept_id == Concept.concept_id')
    person = relationship('Person')
    provider = relationship('Provider')
    unit_concept = relationship('Concept', primaryjoin='Measurement.unit_concept_id == Concept.concept_id')
    value_as_concept = relationship('Concept', primaryjoin='Measurement.value_as_concept_id == Concept.concept_id')
    visit_detail = relationship('VisitDetail')
    visit_occurrence = relationship('VisitOccurrence')


class Note(Base):
    __tablename__ = 'note'
    __table_args__ = {'schema': CDM_SCHEMA}

    note_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    note_event_id = Column(BigInteger)
    note_event_field_concept_id = Column(Integer, nullable=False)
    note_date = Column(Date)
    note_datetime = Column(DateTime, nullable=False)
    note_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    note_class_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    note_title = Column(String(250))
    note_text = Column(Text)
    encoding_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    language_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)
    visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    note_source_value = Column(String(50))

    encoding_concept = relationship('Concept', primaryjoin='Note.encoding_concept_id == Concept.concept_id')
    language_concept = relationship('Concept', primaryjoin='Note.language_concept_id == Concept.concept_id')
    note_class_concept = relationship('Concept', primaryjoin='Note.note_class_concept_id == Concept.concept_id')
    note_type_concept = relationship('Concept', primaryjoin='Note.note_type_concept_id == Concept.concept_id')
    person = relationship('Person')
    provider = relationship('Provider')
    visit_detail = relationship('VisitDetail')
    visit_occurrence = relationship('VisitOccurrence')


class NoteNlp(Base):
    __tablename__ = 'note_nlp'
    __table_args__ = {'schema': CDM_SCHEMA}

    note_nlp_id = Column(BigInteger, primary_key=True)
    note_id = Column(ForeignKey(f'{CDM_SCHEMA}.note.note_id'), nullable=False, index=True)
    section_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    snippet = Column(String(250))
    offset = Column(String(250))
    lexical_variant = Column(String(250), nullable=False)
    note_nlp_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    nlp_system = Column(String(250))
    nlp_date = Column(Date, nullable=False)
    nlp_datetime = Column(DateTime)
    term_exists = Column(String(1))
    term_temporal = Column(String(50))
    term_modifiers = Column(String(2000))
    note_nlp_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    note = relationship('Note')
    note_nlp_concept = relationship('Concept', primaryjoin='NoteNlp.note_nlp_concept_id == Concept.concept_id')
    note_nlp_source_concept = relationship('Concept',
                                           primaryjoin='NoteNlp.note_nlp_source_concept_id == Concept.concept_id')
    section_concept = relationship('Concept', primaryjoin='NoteNlp.section_concept_id == Concept.concept_id')


class Observation(Base):
    __tablename__ = 'observation'
    __table_args__ = {'schema': CDM_SCHEMA}

    observation_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    observation_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    observation_date = Column(Date)
    observation_datetime = Column(DateTime, nullable=False)
    observation_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    value_as_number = Column(Numeric)
    value_as_string = Column(String(60))
    value_as_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    value_as_datetime = Column(DateTime)
    qualifier_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    unit_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)
    visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    observation_source_value = Column(String(50))
    observation_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    unit_source_value = Column(String(50))
    qualifier_source_value = Column(String(50))
    observation_event_id = Column(BigInteger)
    obs_event_field_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    observation_concept = relationship('Concept',
                                       primaryjoin='Observation.observation_concept_id == Concept.concept_id')
    observation_source_concept = relationship('Concept',
                                              primaryjoin='Observation.observation_source_concept_id == Concept.concept_id')
    observation_type_concept = relationship('Concept',
                                            primaryjoin='Observation.observation_type_concept_id == Concept.concept_id')
    obs_event_field_concept = relationship('Concept',
                                           primaryjoin='Observation.obs_event_field_concept_id == Concept.concept_id')
    person = relationship('Person')
    provider = relationship('Provider')
    qualifier_concept = relationship('Concept', primaryjoin='Observation.qualifier_concept_id == Concept.concept_id')
    unit_concept = relationship('Concept', primaryjoin='Observation.unit_concept_id == Concept.concept_id')
    value_as_concept = relationship('Concept', primaryjoin='Observation.value_as_concept_id == Concept.concept_id')
    visit_detail = relationship('VisitDetail')
    visit_occurrence = relationship('VisitOccurrence')


class ObservationPeriod(Base):
    __tablename__ = 'observation_period'
    __table_args__ = {'schema': CDM_SCHEMA}

    observation_period_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    observation_period_start_date = Column(Date, nullable=False)
    observation_period_end_date = Column(Date, nullable=False)
    period_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    period_type_concept = relationship('Concept')
    person = relationship('Person')


class Person(Base):
    __tablename__ = 'person'
    __table_args__ = {'schema': CDM_SCHEMA}

    person_id = Column(BigInteger, primary_key=True, unique=True)
    gender_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    year_of_birth = Column(Integer, nullable=False)
    month_of_birth = Column(Integer)
    day_of_birth = Column(Integer)
    birth_datetime = Column(DateTime)
    death_datetime = Column(DateTime)
    race_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    ethnicity_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    location_id = Column(ForeignKey(f'{CDM_SCHEMA}.location.location_id'))
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    care_site_id = Column(ForeignKey(f'{CDM_SCHEMA}.care_site.care_site_id'))
    person_source_value = Column(String(50))
    gender_source_value = Column(String(50))
    gender_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    race_source_value = Column(String(50))
    race_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    ethnicity_source_value = Column(String(50))
    ethnicity_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    care_site = relationship('CareSite')
    ethnicity_concept = relationship('Concept', primaryjoin='Person.ethnicity_concept_id == Concept.concept_id')
    ethnicity_source_concept = relationship('Concept',
                                            primaryjoin='Person.ethnicity_source_concept_id == Concept.concept_id')
    gender_concept = relationship('Concept', primaryjoin='Person.gender_concept_id == Concept.concept_id')
    gender_source_concept = relationship('Concept', primaryjoin='Person.gender_source_concept_id == Concept.concept_id')
    location = relationship('Location')
    provider = relationship('Provider')
    race_concept = relationship('Concept', primaryjoin='Person.race_concept_id == Concept.concept_id')
    race_source_concept = relationship('Concept', primaryjoin='Person.race_source_concept_id == Concept.concept_id')


class Death(Base):
    """
    A v5 table, reinstated for v6 for backwards compatibility
    """
    __tablename__ = 'death'
    __table_args__ = {'schema': CDM_SCHEMA}

    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), primary_key=True, index=True)
    death_date = Column(Date, nullable=False)
    death_datetime = Column(DateTime)
    death_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    cause_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    cause_source_value = Column(String(50))
    cause_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    cause_concept = relationship('Concept', primaryjoin='Death.cause_concept_id == Concept.concept_id')
    cause_source_concept = relationship('Concept', primaryjoin='Death.cause_source_concept_id == Concept.concept_id')
    death_type_concept = relationship('Concept', primaryjoin='Death.death_type_concept_id == Concept.concept_id')


class ProcedureOccurrence(Base):
    __tablename__ = 'procedure_occurrence'
    __table_args__ = {'schema': CDM_SCHEMA}

    procedure_occurrence_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    procedure_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    procedure_date = Column(Date)
    procedure_datetime = Column(DateTime, nullable=False)
    procedure_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    modifier_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    quantity = Column(Integer)
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)
    visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    procedure_source_value = Column(String(50))
    procedure_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    modifier_source_value = Column(String(50))

    modifier_concept = relationship('Concept',
                                    primaryjoin='ProcedureOccurrence.modifier_concept_id == Concept.concept_id')
    person = relationship('Person')
    procedure_concept = relationship('Concept',
                                     primaryjoin='ProcedureOccurrence.procedure_concept_id == Concept.concept_id')
    procedure_source_concept = relationship('Concept',
                                            primaryjoin='ProcedureOccurrence.procedure_source_concept_id == Concept.concept_id')
    procedure_type_concept = relationship('Concept',
                                          primaryjoin='ProcedureOccurrence.procedure_type_concept_id == Concept.concept_id')
    provider = relationship('Provider')
    visit_detail = relationship('VisitDetail')
    visit_occurrence = relationship('VisitOccurrence')


class Specimen(Base):
    __tablename__ = 'specimen'
    __table_args__ = {'schema': CDM_SCHEMA}

    specimen_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    specimen_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    specimen_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    specimen_date = Column(Date)
    specimen_datetime = Column(DateTime, nullable=False)
    quantity = Column(Numeric)
    unit_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    anatomic_site_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    disease_status_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    specimen_source_id = Column(String(50))
    specimen_source_value = Column(String(50))
    unit_source_value = Column(String(50))
    anatomic_site_source_value = Column(String(50))
    disease_status_source_value = Column(String(50))

    anatomic_site_concept = relationship('Concept',
                                         primaryjoin='Specimen.anatomic_site_concept_id == Concept.concept_id')
    disease_status_concept = relationship('Concept',
                                          primaryjoin='Specimen.disease_status_concept_id == Concept.concept_id')
    person = relationship('Person')
    specimen_concept = relationship('Concept', primaryjoin='Specimen.specimen_concept_id == Concept.concept_id')
    specimen_type_concept = relationship('Concept',
                                         primaryjoin='Specimen.specimen_type_concept_id == Concept.concept_id')
    unit_concept = relationship('Concept', primaryjoin='Specimen.unit_concept_id == Concept.concept_id')


class SurveyConduct(Base):
    __tablename__ = 'survey_conduct'
    __table_args__ = {'schema': CDM_SCHEMA}

    survey_conduct_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    survey_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    survey_start_date = Column(Date)
    survey_start_datetime = Column(DateTime)
    survey_end_date = Column(Date)
    survey_end_datetime = Column(DateTime, nullable=False)
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    assisted_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    respondent_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    timing_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    collection_method_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    assisted_source_value = Column(String(50))
    respondent_type_source_value = Column(String(100))
    timing_source_value = Column(String(100))
    collection_method_source_value = Column(String(100))
    survey_source_value = Column(String(100))
    survey_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    survey_source_identifier = Column(String(100))
    validated_survey_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    validated_survey_source_value = Column(String(100))
    survey_version_number = Column(String(20))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'))
    visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    response_visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'))

    assisted_concept = relationship('Concept', primaryjoin='SurveyConduct.assisted_concept_id == Concept.concept_id')
    collection_method_concept = relationship('Concept',
                                             primaryjoin='SurveyConduct.collection_method_concept_id == Concept.concept_id')
    person = relationship('Person')
    provider = relationship('Provider')
    respondent_type_concept = relationship('Concept',
                                           primaryjoin='SurveyConduct.respondent_type_concept_id == Concept.concept_id')
    response_visit_occurrence = relationship('VisitOccurrence',
                                             primaryjoin='SurveyConduct.response_visit_occurrence_id == VisitOccurrence.visit_occurrence_id')
    survey_concept = relationship('Concept', primaryjoin='SurveyConduct.survey_concept_id == Concept.concept_id')
    survey_source_concept = relationship('Concept',
                                         primaryjoin='SurveyConduct.survey_source_concept_id == Concept.concept_id')
    timing_concept = relationship('Concept', primaryjoin='SurveyConduct.timing_concept_id == Concept.concept_id')
    validated_survey_concept = relationship('Concept',
                                            primaryjoin='SurveyConduct.validated_survey_concept_id == Concept.concept_id')
    visit_detail = relationship('VisitDetail')
    visit_occurrence = relationship('VisitOccurrence',
                                    primaryjoin='SurveyConduct.visit_occurrence_id == VisitOccurrence.visit_occurrence_id')


class VisitDetail(Base):
    __tablename__ = 'visit_detail'
    __table_args__ = {'schema': CDM_SCHEMA}

    visit_detail_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    visit_detail_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    visit_detail_start_date = Column(Date)
    visit_detail_start_datetime = Column(DateTime, nullable=False)
    visit_detail_end_date = Column(Date)
    visit_detail_end_datetime = Column(DateTime, nullable=False)
    visit_detail_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    care_site_id = Column(ForeignKey(f'{CDM_SCHEMA}.care_site.care_site_id'))
    discharge_to_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    admitted_from_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    admitted_from_source_value = Column(String(50))
    visit_detail_source_value = Column(String(50))
    visit_detail_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    discharge_to_source_value = Column(String(50))
    preceding_visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    visit_detail_parent_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), nullable=False)

    admitted_from_concept = relationship('Concept',
                                         primaryjoin='VisitDetail.admitted_from_concept_id == Concept.concept_id')
    care_site = relationship('CareSite')
    discharge_to_concept = relationship('Concept',
                                        primaryjoin='VisitDetail.discharge_to_concept_id == Concept.concept_id')
    person = relationship('Person')
    preceding_visit_detail = relationship('VisitDetail', remote_side=[visit_detail_id],
                                          primaryjoin='VisitDetail.preceding_visit_detail_id == VisitDetail.visit_detail_id')
    provider = relationship('Provider')
    visit_detail_concept = relationship('Concept',
                                        primaryjoin='VisitDetail.visit_detail_concept_id == Concept.concept_id')
    visit_detail_parent = relationship('VisitDetail', remote_side=[visit_detail_id],
                                       primaryjoin='VisitDetail.visit_detail_parent_id == VisitDetail.visit_detail_id')
    visit_detail_source_concept = relationship('Concept',
                                               primaryjoin='VisitDetail.visit_detail_source_concept_id == Concept.concept_id')
    visit_detail_type_concept = relationship('Concept',
                                             primaryjoin='VisitDetail.visit_detail_type_concept_id == Concept.concept_id')
    visit_occurrence = relationship('VisitOccurrence')


class VisitOccurrence(Base):
    __tablename__ = 'visit_occurrence'
    __table_args__ = {'schema': CDM_SCHEMA}

    visit_occurrence_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    visit_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    visit_start_date = Column(Date)
    visit_start_datetime = Column(DateTime, nullable=False)
    visit_end_date = Column(Date)
    visit_end_datetime = Column(DateTime, nullable=False)
    visit_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    care_site_id = Column(ForeignKey(f'{CDM_SCHEMA}.care_site.care_site_id'))
    visit_source_value = Column(String(50))
    visit_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    admitted_from_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    admitted_from_source_value = Column(String(50))
    discharge_to_source_value = Column(String(50))
    discharge_to_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    preceding_visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'))
    record_source_value = Column(String(50))

    admitted_from_concept = relationship('Concept',
                                         primaryjoin='VisitOccurrence.admitted_from_concept_id == Concept.concept_id')
    care_site = relationship('CareSite')
    discharge_to_concept = relationship('Concept',
                                        primaryjoin='VisitOccurrence.discharge_to_concept_id == Concept.concept_id')
    person = relationship('Person')
    preceding_visit_occurrence = relationship('VisitOccurrence', remote_side=[visit_occurrence_id])
    provider = relationship('Provider')
    visit_concept = relationship('Concept', primaryjoin='VisitOccurrence.visit_concept_id == Concept.concept_id')
    visit_source_concept = relationship('Concept',
                                        primaryjoin='VisitOccurrence.visit_source_concept_id == Concept.concept_id')
    visit_type_concept = relationship('Concept',
                                      primaryjoin='VisitOccurrence.visit_type_concept_id == Concept.concept_id')


class StemTable(Base):
    __tablename__ = 'stem_table'
    __table_args__ = {'schema': CDM_SCHEMA}

    id = Column(Integer, primary_key=True)
    domain_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.domain.domain_id'),
                       comment='A foreign key identifying the domain this event belongs to. The domain drives the target CDM table this event will be '
                               'recorded in.  If one is not set specify a default domain.')
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    start_date = Column(Date)
    start_datetime = Column(DateTime, nullable=False)
    end_date = Column(Date)
    end_datetime = Column(DateTime)
    verbatim_end_date = Column(Date)
    type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    operator_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    value_as_number = Column(Numeric)
    value_as_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    value_as_string = Column(String(60))
    value_as_datetime = Column(DateTime)
    unit_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    range_low = Column(Numeric)
    range_high = Column(Numeric)
    provider_id = Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))
    visit_occurrence_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)
    visit_detail_id = Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))
    source_value = Column(String(50))
    source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    unit_source_value = Column(String(50))
    value_source_value = Column(String(50))
    stop_reason = Column(String(20))
    refills = Column(Integer)
    quantity = Column(Numeric)
    days_supply = Column(Integer)
    sig = Column(Text)
    route_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    lot_number = Column(String(50))
    route_source_value = Column(String(50))
    dose_unit_source_value = Column(String(50))
    condition_status_source_value = Column(String(50))
    condition_status_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    qualifier_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    qualifier_source_value = Column(String(50))
    modifier_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    unique_device_id = Column(String(50))
    anatomic_site_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    disease_status_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    specimen_source_id = Column(String(50))
    anatomic_site_source_value = Column(String(50))
    disease_status_source_value = Column(String(50))
    event_id = Column(BigInteger)
    event_field_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))
    modifier_source_value = Column(String(50))
    record_source_value = Column(String(100))

    person = relationship('Person')
    provider = relationship('Provider')
    visit_occurrence = relationship('VisitOccurrence')
    concept = relationship('Concept', primaryjoin='StemTable.concept_id == Concept.concept_id')
    source_concept = relationship('Concept', primaryjoin='StemTable.source_concept_id == Concept.concept_id')
    type_concept = relationship('Concept', primaryjoin='StemTable.type_concept_id == Concept.concept_id')
    operator_concept = relationship('Concept', primaryjoin='StemTable.operator_concept_id == Concept.concept_id')
    unit_concept = relationship('Concept', primaryjoin='StemTable.unit_concept_id == Concept.concept_id')
    value_as_concept = relationship('Concept', primaryjoin='StemTable.value_as_concept_id == Concept.concept_id')
    route_concept = relationship('Concept', primaryjoin='StemTable.route_concept_id == Concept.concept_id')
    qualifier_concept = relationship('Concept', primaryjoin='StemTable.qualifier_concept_id == Concept.concept_id')
    modifier_concept = relationship('Concept', primaryjoin='StemTable.modifier_concept_id == Concept.concept_id')
    anatomic_site_concept = relationship('Concept',
                                         primaryjoin='StemTable.anatomic_site_concept_id == Concept.concept_id')
    disease_status_concept = relationship('Concept',
                                          primaryjoin='StemTable.disease_status_concept_id == Concept.concept_id')
    visit_detail = relationship('VisitDetail')
    event_field_concept = relationship('Concept', primaryjoin='StemTable.event_field_concept_id == Concept.concept_id')


class Episode(Base):
    __tablename__ = 'episode'
    __table_args__ = {'schema': CDM_SCHEMA}

    episode_id = Column(BigInteger, primary_key=True)
    person_id = Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)
    episode_start_datetime = Column(DateTime, nullable=False)
    episode_end_datetime = Column(DateTime, nullable=False)
    episode_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)
    episode_parent_id = Column(ForeignKey(f'{CDM_SCHEMA}.episode.episode_id'))
    episode_number = Column(Integer)
    episode_object_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    episode_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    episode_source_value = Column(String(50))
    episode_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    episode_concept = relationship('Concept', primaryjoin='Episode.episode_concept_id == Concept.concept_id')
    episode_object_concept = relationship('Concept',
                                          primaryjoin='Episode.episode_object_concept_id == Concept.concept_id')
    episode_parent = relationship('Episode', remote_side=[episode_id])
    episode_source_concept = relationship('Concept',
                                          primaryjoin='Episode.episode_source_concept_id == Concept.concept_id')
    episode_type_concept = relationship('Concept', primaryjoin='Episode.episode_type_concept_id == Concept.concept_id')
    record_source_value = Column(String(50))
    person = relationship('Person')


class EpisodeEvent(Base):
    __tablename__ = 'episode_event'
    __table_args__ = {'schema': CDM_SCHEMA}

    episode_id = Column(BigInteger, primary_key=True, nullable=False, index=True)
    event_id = Column(BigInteger, primary_key=True, nullable=False)
    event_field_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False,
                                    index=True)

    event_field_concept = relationship('Concept')
