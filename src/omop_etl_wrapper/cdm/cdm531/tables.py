from sqlalchemy.ext.declarative import declarative_base

from .clinical_data import (
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

from .health_system_data import (
    BaseCareSiteCdm531,
    BaseLocationCdm531,
    BaseProviderCdm531,
)

from .health_economics import (
    BaseCostCdm531,
    BasePayerPlanPeriodCdm531,
)

from .derived_elements import (
    BaseCohortCdm531,
    BaseDoseEraCdm531,
    BaseDrugEraCdm531,
    BaseConditionEraCdm531,
)

from .metadata import (
    BaseMetadataCdm531,
    BaseCdmSourceCdm531,
)

from ..vocabularies import (
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
    BaseCohortDefinition,
)

Base_cdm_531 = declarative_base()


class Person(BasePersonCdm531, Base_cdm_531):
    pass


class Death(BaseDeathCdm531, Base_cdm_531):
    pass


class Note(BaseNoteCdm531, Base_cdm_531):
    pass


class Measurement(BaseMeasurementCdm531, Base_cdm_531):
    pass


class NoteNlp(BaseNoteNlpCdm531, Base_cdm_531):
    pass


class Observation(BaseObservationCdm531, Base_cdm_531):
    pass


class Specimen(BaseSpecimenCdm531, Base_cdm_531):
    pass


class StemTable(BaseStemTableCdm531, Base_cdm_531):
    pass


class VisitDetail(BaseVisitDetailCdm531, Base_cdm_531):
    pass


class ConditionOccurrence(BaseConditionOccurrenceCdm531, Base_cdm_531):
    pass


class DeviceExposure(BaseDeviceExposureCdm531, Base_cdm_531):
    pass


class DrugExposure(BaseDrugExposureCdm531, Base_cdm_531):
    pass


class FactRelationship(BaseFactRelationshipCdm531, Base_cdm_531):
    pass


class ObservationPeriod(BaseObservationPeriodCdm531, Base_cdm_531):
    pass


class ProcedureOccurrence(BaseProcedureOccurrenceCdm531, Base_cdm_531):
    pass


class VisitOccurrence(BaseVisitOccurrenceCdm531, Base_cdm_531):
    pass


class Location(BaseLocationCdm531, Base_cdm_531):
    pass


class CareSite(BaseCareSiteCdm531, Base_cdm_531):
    pass


class Provider(BaseProviderCdm531, Base_cdm_531):
    pass


class Cost(BaseCostCdm531, Base_cdm_531):
    pass


class PayerPlanPeriod(BasePayerPlanPeriodCdm531, Base_cdm_531):
    pass


class Cohort(BaseCohortCdm531, Base_cdm_531):
    pass


class DoseEra(BaseDoseEraCdm531, Base_cdm_531):
    pass


class DrugEra(BaseDrugEraCdm531, Base_cdm_531):
    pass


class ConditionEra(BaseConditionEraCdm531, Base_cdm_531):
    pass


class CdmSource(BaseCdmSourceCdm531, Base_cdm_531):
    pass


class Metadata(BaseMetadataCdm531, Base_cdm_531):
    pass


class Concept(BaseConcept, Base_cdm_531):
    pass


class ConceptAncestor(BaseConceptAncestor, Base_cdm_531):
    pass


class ConceptClass(BaseConceptClass, Base_cdm_531):
    pass


class ConceptRelationship(BaseConceptRelationship, Base_cdm_531):
    pass


class ConceptSynonym(BaseConceptSynonym, Base_cdm_531):
    pass


class DrugStrength(BaseDrugStrength, Base_cdm_531):
    pass


class Relationship(BaseRelationship, Base_cdm_531):
    pass


class SourceToConceptMap(BaseSourceToConceptMap, Base_cdm_531):
    pass


class Vocabulary(BaseVocabulary, Base_cdm_531):
    pass


class Domain(BaseDomain, Base_cdm_531):
    pass


class CohortDefinition(BaseCohortDefinition, Base_cdm_531):
    pass
