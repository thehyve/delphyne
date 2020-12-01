from sqlalchemy.ext.declarative import declarative_base

from src.omop_etl_wrapper.cdm.cdm600.clinical_data import (
    BasePersonCdm600,
    BaseNoteCdm600,
    BaseMeasurementCdm600,
    BaseNoteNlpCdm600,
    BaseObservationCdm600,
    BaseSpecimenCdm600,
    BaseStemTableCdm600,
    BaseVisitDetailCdm600,
    BaseConditionOccurrenceCdm600,
    BaseDeviceExposureCdm600,
    BaseDrugExposureCdm600,
    BaseFactRelationshipCdm600,
    BaseObservationPeriodCdm600,
    BaseProcedureOccurrenceCdm600,
    BaseSurveyConductCdm600,
    BaseVisitOccurrenceCdm600,
)

from src.omop_etl_wrapper.cdm.cdm600.health_system_data import (
    BaseCareSiteCdm600,
    BaseLocationCdm600,
    BaseProviderCdm600,
    BaseLocationHistoryCdm600,
)

from src.omop_etl_wrapper.cdm.cdm600.health_economics import (
    BaseCostCdm600,
    BasePayerPlanPeriodCdm600,
)

from src.omop_etl_wrapper.cdm.cdm600.derived_elements import (
    BaseDoseEraCdm600,
    BaseDrugEraCdm600,
    BaseConditionEraCdm600,
)

from src.omop_etl_wrapper.cdm.metadata import (
    BaseMetadata,
    BaseCdmSource,
)

from src.omop_etl_wrapper.cdm.vocabularies import (
    BaseVocabulary,
    BaseSourceToConceptMap,
    BaseConcept,
    BaseConceptAncestor,
    BaseConceptClass,
    BaseConceptRelationship,
    BaseConceptSynonym,
    BaseDomain,
    BaseDrugStrength,
    BaseRelationship,
)


Base = declarative_base()


class Person(BasePersonCdm600, Base):
    pass


class Note(BaseNoteCdm600, Base):
    pass


class Measurement(BaseMeasurementCdm600, Base):
    pass


class NoteNlp(BaseNoteNlpCdm600, Base):
    pass


class Observation(BaseObservationCdm600, Base):
    pass


class Specimen(BaseSpecimenCdm600, Base):
    pass


class StemTable(BaseStemTableCdm600, Base):
    pass


class VisitDetail(BaseVisitDetailCdm600, Base):
    pass


class ConditionOccurrence(BaseConditionOccurrenceCdm600, Base):
    pass


class DeviceExposure(BaseDeviceExposureCdm600, Base):
    pass


class DrugExposure(BaseDrugExposureCdm600, Base):
    pass


class FactRelationship(BaseFactRelationshipCdm600, Base):
    pass


class ObservationPeriod(BaseObservationPeriodCdm600, Base):
    pass


class ProcedureOccurrence(BaseProcedureOccurrenceCdm600, Base):
    pass


class SurveyConduct(BaseSurveyConductCdm600, Base):
    pass


class VisitOccurrence(BaseVisitOccurrenceCdm600, Base):
    pass


class Cost(BaseCostCdm600, Base):
    pass


class PayerPlanPeriod(BasePayerPlanPeriodCdm600, Base):
    pass


class CdmSource(BaseCdmSource, Base):
    pass


class Metadata(BaseMetadata, Base):
    pass


class DoseEra(BaseDoseEraCdm600, Base):
    pass


class DrugEra(BaseDrugEraCdm600, Base):
    pass


class ConditionEra(BaseConditionEraCdm600, Base):
    pass


class Location(BaseLocationCdm600, Base):
    pass


class CareSite(BaseCareSiteCdm600, Base):
    pass


class Provider(BaseProviderCdm600, Base):
    pass


class LocationHistory(BaseLocationHistoryCdm600, Base):
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


class Vocabulary(BaseVocabulary, Base):
    pass


class Domain(BaseDomain, Base):
    pass
