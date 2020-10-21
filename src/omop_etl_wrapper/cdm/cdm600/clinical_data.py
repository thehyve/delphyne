from sqlalchemy import (BigInteger, Column, Date, DateTime, ForeignKey,
                        Numeric, String, Integer, Text)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

from .._schema_placeholders import VOCAB_SCHEMA, CDM_SCHEMA


class BaseConditionOccurrenceCdm600:
    __tablename__ = 'condition_occurrence'
    __table_args__ = {'schema': CDM_SCHEMA}

    condition_occurrence_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def condition_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    condition_start_date = Column(Date)
    condition_start_datetime = Column(DateTime, nullable=False)
    condition_end_date = Column(Date)
    condition_end_datetime = Column(DateTime)

    @declared_attr
    def condition_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    stop_reason = Column(String(20))

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    condition_source_value = Column(String(50))

    @declared_attr
    def condition_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    condition_status_source_value = Column(String(50))

    @declared_attr
    def condition_status_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def condition_concept(cls):
        return relationship('Concept',
                            primaryjoin='ConditionOccurrence.condition_concept_id == Concept.concept_id')

    @declared_attr
    def condition_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='ConditionOccurrence.condition_source_concept_id == Concept.concept_id')

    @declared_attr
    def condition_status_concept(cls):
        return relationship('Concept',
                            primaryjoin='ConditionOccurrence.condition_status_concept_id == Concept.concept_id')

    @declared_attr
    def condition_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='ConditionOccurrence.condition_type_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')


class BaseDeviceExposureCdm600:
    __tablename__ = 'device_exposure'
    __table_args__ = {'schema': CDM_SCHEMA}

    device_exposure_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def device_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    device_exposure_start_date = Column(Date)
    device_exposure_start_datetime = Column(DateTime, nullable=False)
    device_exposure_end_date = Column(Date)
    device_exposure_end_datetime = Column(DateTime)

    @declared_attr
    def device_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    unique_device_id = Column(String(50))
    quantity = Column(Integer)

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    device_source_value = Column(String(100))

    @declared_attr
    def device_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def device_concept(cls):
        return relationship('Concept', primaryjoin='DeviceExposure.device_concept_id == Concept.concept_id')

    @declared_attr
    def device_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='DeviceExposure.device_source_concept_id == Concept.concept_id')

    @declared_attr
    def device_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='DeviceExposure.device_type_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')


class BaseDrugExposureCdm600:
    __tablename__ = 'drug_exposure'
    __table_args__ = {'schema': CDM_SCHEMA}

    drug_exposure_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def drug_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    drug_exposure_start_date = Column(Date)
    drug_exposure_start_datetime = Column(DateTime, nullable=False)
    drug_exposure_end_date = Column(Date)
    drug_exposure_end_datetime = Column(DateTime, nullable=False)
    verbatim_end_date = Column(Date)

    @declared_attr
    def drug_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    stop_reason = Column(String(20))
    refills = Column(Integer)
    quantity = Column(Numeric)
    days_supply = Column(Integer)
    sig = Column(Text)

    @declared_attr
    def route_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    lot_number = Column(String(50))

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    drug_source_value = Column(String(50))

    @declared_attr
    def drug_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    route_source_value = Column(String(50))
    dose_unit_source_value = Column(String(50))

    @declared_attr
    def drug_concept(cls):
        return relationship('Concept', primaryjoin='DrugExposure.drug_concept_id == Concept.concept_id')

    @declared_attr
    def drug_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='DrugExposure.drug_source_concept_id == Concept.concept_id')

    @declared_attr
    def drug_type_concept(cls):
        return relationship('Concept', primaryjoin='DrugExposure.drug_type_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def route_concept(cls):
        return relationship('Concept', primaryjoin='DrugExposure.route_concept_id == Concept.concept_id')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')


class BaseFactRelationshipCdm600:
    __tablename__ = 'fact_relationship'
    __table_args__ = {'schema': CDM_SCHEMA}

    fact_relationship_id = Column(Integer, primary_key=True)

    @declared_attr
    def domain_concept_id_1(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    fact_id_1 = Column(Integer, nullable=False)

    @declared_attr
    def domain_concept_id_2(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    fact_id_2 = Column(Integer, nullable=False)

    @declared_attr
    def relationship_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)


class BaseNoteCdm600:
    __tablename__ = 'note'
    __table_args__ = {'schema': CDM_SCHEMA}

    note_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    note_event_id = Column(BigInteger)
    note_event_field_concept_id = Column(Integer, nullable=False)
    note_date = Column(Date)
    note_datetime = Column(DateTime, nullable=False)

    @declared_attr
    def note_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    @declared_attr
    def note_class_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    note_title = Column(String(250))
    note_text = Column(Text)

    @declared_attr
    def encoding_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def language_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    note_source_value = Column(String(50))

    @declared_attr
    def encoding_concept(cls):
        return relationship('Concept', primaryjoin='Note.encoding_concept_id == Concept.concept_id')

    @declared_attr
    def language_concept(cls):
        return relationship('Concept', primaryjoin='Note.language_concept_id == Concept.concept_id')

    @declared_attr
    def note_class_concept(cls):
        return relationship('Concept', primaryjoin='Note.note_class_concept_id == Concept.concept_id')

    @declared_attr
    def note_type_concept(cls):
        return relationship('Concept', primaryjoin='Note.note_type_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')


class BaseNoteNlpCdm600:
    __tablename__ = 'note_nlp'
    __table_args__ = {'schema': CDM_SCHEMA}

    note_nlp_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def note_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.note.note_id'), nullable=False, index=True)

    @declared_attr
    def section_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    snippet = Column(String(250))
    offset = Column(String(250))
    lexical_variant = Column(String(250), nullable=False)

    @declared_attr
    def note_nlp_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    nlp_system = Column(String(250))
    nlp_date = Column(Date, nullable=False)
    nlp_datetime = Column(DateTime)
    term_exists = Column(String(1))
    term_temporal = Column(String(50))
    term_modifiers = Column(String(2000))

    @declared_attr
    def note_nlp_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def note(cls):
        return relationship('Note')

    @declared_attr
    def note_nlp_concept(cls):
        return relationship('Concept', primaryjoin='NoteNlp.note_nlp_concept_id == Concept.concept_id')

    @declared_attr
    def note_nlp_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='NoteNlp.note_nlp_source_concept_id == Concept.concept_id')

    @declared_attr
    def section_concept(cls):
        return relationship('Concept', primaryjoin='NoteNlp.section_concept_id == Concept.concept_id')


class BaseObservationCdm600:
    __tablename__ = 'observation'
    __table_args__ = {'schema': CDM_SCHEMA}

    observation_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def observation_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    observation_date = Column(Date)
    observation_datetime = Column(DateTime, nullable=False)

    @declared_attr
    def observation_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    value_as_number = Column(Numeric)
    value_as_string = Column(String(60))

    @declared_attr
    def value_as_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    value_as_datetime = Column(DateTime)

    @declared_attr
    def qualifier_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    @declared_attr
    def unit_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    observation_source_value = Column(String(50))

    @declared_attr
    def observation_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    unit_source_value = Column(String(50))
    qualifier_source_value = Column(String(50))
    observation_event_id = Column(BigInteger)

    @declared_attr
    def obs_event_field_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def observation_concept(cls):
        return relationship('Concept',
                            primaryjoin='Observation.observation_concept_id == Concept.concept_id')

    @declared_attr
    def observation_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='Observation.observation_source_concept_id == Concept.concept_id')

    @declared_attr
    def observation_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='Observation.observation_type_concept_id == Concept.concept_id')

    @declared_attr
    def obs_event_field_concept(cls):
        return relationship('Concept',
                            primaryjoin='Observation.obs_event_field_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def qualifier_concept(cls):
        return relationship('Concept', primaryjoin='Observation.qualifier_concept_id == Concept.concept_id')

    @declared_attr
    def unit_concept(cls):
        return relationship('Concept', primaryjoin='Observation.unit_concept_id == Concept.concept_id')

    @declared_attr
    def value_as_concept(cls):
        return relationship('Concept', primaryjoin='Observation.value_as_concept_id == Concept.concept_id')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')


class BaseObservationPeriodCdm600:
    __tablename__ = 'observation_period'
    __table_args__ = {'schema': CDM_SCHEMA}

    observation_period_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    observation_period_start_date = Column(Date, nullable=False)
    observation_period_end_date = Column(Date, nullable=False)

    @declared_attr
    def period_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def period_type_concept(cls):
        return relationship('Concept')

    @declared_attr
    def person(cls):
        return relationship('Person')


class BasePersonCdm600:
    __tablename__ = 'person'
    __table_args__ = {'schema': CDM_SCHEMA}

    person_id = Column(BigInteger, primary_key=True, unique=True)

    @declared_attr
    def gender_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    year_of_birth = Column(Integer, nullable=False)
    month_of_birth = Column(Integer)
    day_of_birth = Column(Integer)
    birth_datetime = Column(DateTime)
    death_datetime = Column(DateTime)

    @declared_attr
    def race_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def ethnicity_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def location_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.location.location_id'))

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def care_site_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.care_site.care_site_id'))

    person_source_value = Column(String(50))
    gender_source_value = Column(String(50))

    @declared_attr
    def gender_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    race_source_value = Column(String(50))

    @declared_attr
    def race_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    ethnicity_source_value = Column(String(50))

    @declared_attr
    def ethnicity_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def care_site(cls):
        return relationship('CareSite')

    @declared_attr
    def ethnicity_concept(cls):
        return relationship('Concept', primaryjoin='Person.ethnicity_concept_id == Concept.concept_id')

    @declared_attr
    def ethnicity_source_concept(cls):
        return relationship('Concept', primaryjoin='Person.ethnicity_source_concept_id == Concept.concept_id')

    @declared_attr
    def gender_concept(cls):
        return relationship('Concept', primaryjoin='Person.gender_concept_id == Concept.concept_id')

    @declared_attr
    def gender_source_concept(cls):
        return relationship('Concept', primaryjoin='Person.gender_source_concept_id == Concept.concept_id')

    @declared_attr
    def location(cls):
        return relationship('Location')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def race_concept(cls):
        return relationship('Concept', primaryjoin='Person.race_concept_id == Concept.concept_id')

    @declared_attr
    def race_source_concept(cls):
        return relationship('Concept', primaryjoin='Person.race_source_concept_id == Concept.concept_id')


class BaseProcedureOccurrenceCdm600:
    __tablename__ = 'procedure_occurrence'
    __table_args__ = {'schema': CDM_SCHEMA}

    procedure_occurrence_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def procedure_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    procedure_date = Column(Date)
    procedure_datetime = Column(DateTime, nullable=False)

    @declared_attr
    def procedure_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def modifier_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    quantity = Column(Integer)

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    procedure_source_value = Column(String(50))

    @declared_attr
    def procedure_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    modifier_source_value = Column(String(50))

    @declared_attr
    def modifier_concept(cls):
        return relationship('Concept',
                            primaryjoin='ProcedureOccurrence.modifier_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def procedure_concept(cls):
        return relationship('Concept',
                            primaryjoin='ProcedureOccurrence.procedure_concept_id == Concept.concept_id')

    @declared_attr
    def procedure_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='ProcedureOccurrence.procedure_source_concept_id == Concept.concept_id')

    @declared_attr
    def procedure_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='ProcedureOccurrence.procedure_type_concept_id == Concept.concept_id')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')


class BaseSpecimenCdm600:
    __tablename__ = 'specimen'
    __table_args__ = {'schema': CDM_SCHEMA}

    specimen_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def specimen_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    @declared_attr
    def specimen_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    specimen_date = Column(Date)
    specimen_datetime = Column(DateTime, nullable=False)
    quantity = Column(Numeric)

    @declared_attr
    def unit_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    @declared_attr
    def anatomic_site_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def disease_status_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    specimen_source_id = Column(String(50))
    specimen_source_value = Column(String(50))
    unit_source_value = Column(String(50))
    anatomic_site_source_value = Column(String(50))
    disease_status_source_value = Column(String(50))

    @declared_attr
    def anatomic_site_concept(cls):
        return relationship('Concept',
                            primaryjoin='Specimen.anatomic_site_concept_id == Concept.concept_id')

    @declared_attr
    def disease_status_concept(cls):
        return relationship('Concept',
                            primaryjoin='Specimen.disease_status_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def specimen_concept(cls):
        return relationship('Concept', primaryjoin='Specimen.specimen_concept_id == Concept.concept_id')

    @declared_attr
    def specimen_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='Specimen.specimen_type_concept_id == Concept.concept_id')

    @declared_attr
    def unit_concept(cls):
        return relationship('Concept', primaryjoin='Specimen.unit_concept_id == Concept.concept_id')


class BaseSurveyConductCdm600:
    __tablename__ = 'survey_conduct'
    __table_args__ = {'schema': CDM_SCHEMA}

    survey_conduct_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def survey_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    survey_start_date = Column(Date)
    survey_start_datetime = Column(DateTime)
    survey_end_date = Column(Date)
    survey_end_datetime = Column(DateTime, nullable=False)

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def assisted_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def respondent_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def timing_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def collection_method_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    assisted_source_value = Column(String(50))
    respondent_type_source_value = Column(String(100))
    timing_source_value = Column(String(100))
    collection_method_source_value = Column(String(100))
    survey_source_value = Column(String(100))

    @declared_attr
    def survey_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    survey_source_identifier = Column(String(100))

    @declared_attr
    def validated_survey_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    validated_survey_source_value = Column(String(100))
    survey_version_number = Column(String(20))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'))

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    @declared_attr
    def response_visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'))

    @declared_attr
    def assisted_concept(cls):
        return relationship('Concept', primaryjoin='SurveyConduct.assisted_concept_id == Concept.concept_id')

    @declared_attr
    def collection_method_concept(cls):
        return relationship('Concept',
                            primaryjoin='SurveyConduct.collection_method_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def respondent_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='SurveyConduct.respondent_type_concept_id == Concept.concept_id')

    @declared_attr
    def response_visit_occurrence(cls):
        return relationship('VisitOccurrence',
                            primaryjoin='SurveyConduct.response_visit_occurrence_id == VisitOccurrence.visit_occurrence_id')

    @declared_attr
    def survey_concept(cls):
        return relationship('Concept', primaryjoin='SurveyConduct.survey_concept_id == Concept.concept_id')

    @declared_attr
    def survey_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='SurveyConduct.survey_source_concept_id == Concept.concept_id')

    @declared_attr
    def timing_concept(cls):
        return relationship('Concept', primaryjoin='SurveyConduct.timing_concept_id == Concept.concept_id')

    @declared_attr
    def validated_survey_concept(cls):
        return relationship('Concept',
                            primaryjoin='SurveyConduct.validated_survey_concept_id == Concept.concept_id')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence',
                            primaryjoin='SurveyConduct.visit_occurrence_id == VisitOccurrence.visit_occurrence_id')


class BaseVisitDetailCdm600:
    __tablename__ = 'visit_detail'
    __table_args__ = {'schema': CDM_SCHEMA}

    visit_detail_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def visit_detail_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    visit_detail_start_date = Column(Date)
    visit_detail_start_datetime = Column(DateTime, nullable=False)
    visit_detail_end_date = Column(Date)
    visit_detail_end_datetime = Column(DateTime, nullable=False)

    @declared_attr
    def visit_detail_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def care_site_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.care_site.care_site_id'))

    @declared_attr
    def discharge_to_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def admitted_from_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    admitted_from_source_value = Column(String(50))
    visit_detail_source_value = Column(String(50))

    @declared_attr
    def visit_detail_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    discharge_to_source_value = Column(String(50))

    @declared_attr
    def preceding_visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    @declared_attr
    def visit_detail_parent_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), nullable=False)

    @declared_attr
    def admitted_from_concept(cls):
        return relationship('Concept',
                            primaryjoin='VisitDetail.admitted_from_concept_id == Concept.concept_id')

    @declared_attr
    def care_site(cls):
        return relationship('CareSite')

    @declared_attr
    def discharge_to_concept(cls):
        return relationship('Concept',
                            primaryjoin='VisitDetail.discharge_to_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def preceding_visit_detail(cls):
        return relationship('VisitDetail', remote_side=[cls.visit_detail_id],
                            primaryjoin='VisitDetail.preceding_visit_detail_id == VisitDetail.visit_detail_id')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def visit_detail_concept(cls):
        return relationship('Concept',
                            primaryjoin='VisitDetail.visit_detail_concept_id == Concept.concept_id')

    @declared_attr
    def visit_detail_parent(cls):
        return relationship('VisitDetail', remote_side=[cls.visit_detail_id],
                            primaryjoin='VisitDetail.visit_detail_parent_id == VisitDetail.visit_detail_id')

    @declared_attr
    def visit_detail_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='VisitDetail.visit_detail_source_concept_id == Concept.concept_id')

    @declared_attr
    def visit_detail_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='VisitDetail.visit_detail_type_concept_id == Concept.concept_id')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')


class BaseVisitOccurrenceCdm600:
    __tablename__ = 'visit_occurrence'
    __table_args__ = {'schema': CDM_SCHEMA}

    visit_occurrence_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def visit_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    visit_start_date = Column(Date)
    visit_start_datetime = Column(DateTime, nullable=False)
    visit_end_date = Column(Date)
    visit_end_datetime = Column(DateTime, nullable=False)

    @declared_attr
    def visit_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def care_site_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.care_site.care_site_id'))

    visit_source_value = Column(String(50))

    @declared_attr
    def visit_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def admitted_from_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    admitted_from_source_value = Column(String(50))
    discharge_to_source_value = Column(String(50))

    @declared_attr
    def discharge_to_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def preceding_visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'))


    @declared_attr
    def admitted_from_concept(cls):
        return relationship('Concept',
                            primaryjoin='VisitOccurrence.admitted_from_concept_id == Concept.concept_id')

    @declared_attr
    def care_site(cls):
        return relationship('CareSite')

    @declared_attr
    def discharge_to_concept(cls):
        return relationship('Concept',
                            primaryjoin='VisitOccurrence.discharge_to_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def preceding_visit_occurrence(cls):
        return relationship('VisitOccurrence', remote_side=[cls.visit_occurrence_id])

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def visit_concept(cls):
        return relationship('Concept', primaryjoin='VisitOccurrence.visit_concept_id == Concept.concept_id')

    @declared_attr
    def visit_source_concept(cls):
        return relationship('Concept',
                            primaryjoin='VisitOccurrence.visit_source_concept_id == Concept.concept_id')

    @declared_attr
    def visit_type_concept(cls):
        return relationship('Concept',
                            primaryjoin='VisitOccurrence.visit_type_concept_id == Concept.concept_id')


class BaseStemTableCdm600:
    __tablename__ = 'stem_table'
    __table_args__ = {'schema': CDM_SCHEMA}

    id = Column(Integer, primary_key=True)

    @declared_attr
    def domain_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.domain.domain_id'),
                      comment='A foreign key identifying the domain this event belongs to. '
                              'The domain drives the target CDM table this event will be '
                              'recorded in.  If one is not set specify a default domain.')

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    start_date = Column(Date)
    start_datetime = Column(DateTime, nullable=False)
    end_date = Column(Date)
    end_datetime = Column(DateTime)
    verbatim_end_date = Column(Date)

    @declared_attr
    def type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def operator_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    value_as_number = Column(Numeric)

    @declared_attr
    def value_as_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    value_as_string = Column(String(60))
    value_as_datetime = Column(DateTime)

    @declared_attr
    def unit_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    range_low = Column(Numeric)
    range_high = Column(Numeric)

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    source_value = Column(String(50))

    @declared_attr
    def source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    unit_source_value = Column(String(50))
    value_source_value = Column(String(50))
    stop_reason = Column(String(20))
    refills = Column(Integer)
    quantity = Column(Numeric)
    days_supply = Column(Integer)
    sig = Column(Text)

    @declared_attr
    def route_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    lot_number = Column(String(50))
    route_source_value = Column(String(50))
    dose_unit_source_value = Column(String(50))
    condition_status_source_value = Column(String(50))

    @declared_attr
    def condition_status_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    @declared_attr
    def qualifier_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    qualifier_source_value = Column(String(50))

    @declared_attr
    def modifier_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    unique_device_id = Column(String(50))

    @declared_attr
    def anatomic_site_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    @declared_attr
    def disease_status_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    specimen_source_id = Column(String(50))
    anatomic_site_source_value = Column(String(50))
    disease_status_source_value = Column(String(50))
    event_id = Column(BigInteger)

    @declared_attr
    def event_field_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    modifier_source_value = Column(String(50))

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')

    @declared_attr
    def concept(cls):
        return relationship('Concept', primaryjoin='StemTable.concept_id == Concept.concept_id')

    @declared_attr
    def source_concept(cls):
        return relationship('Concept', primaryjoin='StemTable.source_concept_id == Concept.concept_id')

    @declared_attr
    def type_concept(cls):
        return relationship('Concept', primaryjoin='StemTable.type_concept_id == Concept.concept_id')

    @declared_attr
    def operator_concept(cls):
        return relationship('Concept', primaryjoin='StemTable.operator_concept_id == Concept.concept_id')

    @declared_attr
    def unit_concept(cls):
        return relationship('Concept', primaryjoin='StemTable.unit_concept_id == Concept.concept_id')

    @declared_attr
    def value_as_concept(cls):
        return relationship('Concept', primaryjoin='StemTable.value_as_concept_id == Concept.concept_id')

    @declared_attr
    def route_concept(cls):
        return relationship('Concept', primaryjoin='StemTable.route_concept_id == Concept.concept_id')

    @declared_attr
    def qualifier_concept(cls):
        return relationship('Concept', primaryjoin='StemTable.qualifier_concept_id == Concept.concept_id')

    @declared_attr
    def modifier_concept(cls):
        return relationship('Concept', primaryjoin='StemTable.modifier_concept_id == Concept.concept_id')

    @declared_attr
    def anatomic_site_concept(cls):
        return relationship('Concept',
                            primaryjoin='StemTable.anatomic_site_concept_id == Concept.concept_id')

    @declared_attr
    def disease_status_concept(cls):
        return relationship('Concept',
                            primaryjoin='StemTable.disease_status_concept_id == Concept.concept_id')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def event_field_concept(cls):
        return relationship('Concept', primaryjoin='StemTable.event_field_concept_id == Concept.concept_id')


class BaseMeasurementCdm600:
    __tablename__ = 'measurement'
    __table_args__ = {'schema': CDM_SCHEMA}

    measurement_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def person_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.person.person_id'), nullable=False, index=True)

    @declared_attr
    def measurement_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False, index=True)

    measurement_date = Column(Date)
    measurement_datetime = Column(DateTime, nullable=False)
    measurement_time = Column(String(10))

    @declared_attr
    def measurement_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def operator_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    value_as_number = Column(Numeric)

    @declared_attr
    def value_as_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    @declared_attr
    def unit_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    range_low = Column(Numeric)
    range_high = Column(Numeric)

    @declared_attr
    def provider_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.provider.provider_id'))

    @declared_attr
    def visit_occurrence_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_occurrence.visit_occurrence_id'), index=True)

    @declared_attr
    def visit_detail_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.visit_detail.visit_detail_id'))

    measurement_source_value = Column(String(50))

    @declared_attr
    def measurement_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    unit_source_value = Column(String(50))
    value_source_value = Column(String(50))

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
    def operator_concept(cls):
        return relationship('Concept', primaryjoin='Measurement.operator_concept_id == Concept.concept_id')

    @declared_attr
    def person(cls):
        return relationship('Person')

    @declared_attr
    def provider(cls):
        return relationship('Provider')

    @declared_attr
    def unit_concept(cls):
        return relationship('Concept', primaryjoin='Measurement.unit_concept_id == Concept.concept_id')

    @declared_attr
    def value_as_concept(cls):
        return relationship('Concept', primaryjoin='Measurement.value_as_concept_id == Concept.concept_id')

    @declared_attr
    def visit_detail(cls):
        return relationship('VisitDetail')

    @declared_attr
    def visit_occurrence(cls):
        return relationship('VisitOccurrence')
