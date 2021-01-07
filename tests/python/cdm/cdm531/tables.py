from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from src.omop_etl_wrapper.database.constraints import NAMING_CONVENTION

from src.omop_etl_wrapper.cdm.cdm531.clinical_data import (
    BasePersonCdm531,
    BaseDeathCdm531,
    BaseNoteCdm531,
    BaseMeasurementCdm531,
    BaseNoteNlpCdm531,
    BaseObservationCdm531,
    BaseSpecimenCdm531,
    BaseStemTableCdm531,
    BaseVisitDetailCdm531,
    BaseConditionOccurrenceCdm531,
    BaseDeviceExposureCdm531,
    BaseDrugExposureCdm531,
    BaseFactRelationshipCdm531,
    BaseObservationPeriodCdm531,
    BaseProcedureOccurrenceCdm531,
    BaseVisitOccurrenceCdm531,
)

from src.omop_etl_wrapper.cdm.cdm531.health_system_data import (
    BaseCareSiteCdm531,
    BaseLocationCdm531,
    BaseProviderCdm531,
)

from src.omop_etl_wrapper.cdm.cdm531.health_economics import (
    BaseCostCdm531,
    BasePayerPlanPeriodCdm531,
)

from src.omop_etl_wrapper.cdm.cdm531.derived_elements import (
    BaseCohortCdm531,
    BaseDoseEraCdm531,
    BaseDrugEraCdm531,
    BaseConditionEraCdm531,
)

from src.omop_etl_wrapper.cdm.metadata import (
    BaseMetadata,
    BaseCdmSource,
)

from src.omop_etl_wrapper.cdm.vocabularies import (
    BaseVocabulary,
    BaseSourceToConceptMap,
    BaseSourceToConceptMapVersion,
    BaseConcept,
    BaseConceptAncestor,
    BaseConceptClass,
    BaseConceptRelationship,
    BaseConceptSynonym,
    BaseDomain,
    BaseDrugStrength,
    BaseRelationship,
    BaseCohortDefinition,
)

Base = declarative_base()
Base.metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Person(BasePersonCdm531, Base):
    pass


class Death(BaseDeathCdm531, Base):
    pass


class Note(BaseNoteCdm531, Base):
    pass


class Measurement(BaseMeasurementCdm531, Base):
    pass


class NoteNlp(BaseNoteNlpCdm531, Base):
    pass


class Observation(BaseObservationCdm531, Base):
    pass


class Specimen(BaseSpecimenCdm531, Base):
    pass


class StemTable(BaseStemTableCdm531, Base):
    pass


class VisitDetail(BaseVisitDetailCdm531, Base):
    pass


class ConditionOccurrence(BaseConditionOccurrenceCdm531, Base):
    pass


class DeviceExposure(BaseDeviceExposureCdm531, Base):
    pass


class DrugExposure(BaseDrugExposureCdm531, Base):
    pass


class FactRelationship(BaseFactRelationshipCdm531, Base):
    pass


class ObservationPeriod(BaseObservationPeriodCdm531, Base):
    pass


class ProcedureOccurrence(BaseProcedureOccurrenceCdm531, Base):
    pass


class VisitOccurrence(BaseVisitOccurrenceCdm531, Base):
    pass


class Location(BaseLocationCdm531, Base):
    pass


class CareSite(BaseCareSiteCdm531, Base):
    pass


class Provider(BaseProviderCdm531, Base):
    pass


class Cost(BaseCostCdm531, Base):
    pass


class PayerPlanPeriod(BasePayerPlanPeriodCdm531, Base):
    pass


class Cohort(BaseCohortCdm531, Base):
    pass


class DoseEra(BaseDoseEraCdm531, Base):
    pass


class DrugEra(BaseDrugEraCdm531, Base):
    pass


class ConditionEra(BaseConditionEraCdm531, Base):
    pass


class CdmSource(BaseCdmSource, Base):
    pass


class Metadata(BaseMetadata, Base):
    pass


class Concept(BaseConcept, Base):
    pass


class ConceptAncestor(BaseConceptAncestor, Base):
    pass


class ConceptClass(BaseConceptClass, Base):
    pass


class ConceptRelationship(BaseConceptRelationship, Base):
    pass


class ConceptSynonym(BaseConceptSynonym, Base):
    pass


class DrugStrength(BaseDrugStrength, Base):
    pass


class Relationship(BaseRelationship, Base):
    pass


class SourceToConceptMap(BaseSourceToConceptMap, Base):
    pass


class SourceToConceptMapVersion(BaseSourceToConceptMapVersion, Base):
    pass


class Vocabulary(BaseVocabulary, Base):
    pass


class Domain(BaseDomain, Base):
    pass


class CohortDefinition(BaseCohortDefinition, Base):
    pass
