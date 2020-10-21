from sqlalchemy import (BigInteger, Column, Date, ForeignKey, Integer,
                        Numeric, String)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

from .._schema_placeholders import VOCAB_SCHEMA, CDM_SCHEMA


class BaseCareSiteCdm600:
    __tablename__ = 'care_site'
    __table_args__ = {'schema': CDM_SCHEMA}

    care_site_id = Column(BigInteger, primary_key=True)
    care_site_name = Column(String(255))

    @declared_attr
    def place_of_service_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def location_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.location.location_id'))

    care_site_source_value = Column(String(50))
    place_of_service_source_value = Column(String(50))

    @declared_attr
    def location(cls):
        return relationship('Location')

    @declared_attr
    def place_of_service_concept(cls):
        return relationship('Concept')


class BaseLocationCdm600:
    __tablename__ = 'location'
    __table_args__ = {'schema': CDM_SCHEMA}

    location_id = Column(BigInteger, primary_key=True)
    address_1 = Column(String(50))
    address_2 = Column(String(50))
    city = Column(String(50))
    state = Column(String(2))
    zip = Column(String(9))
    county = Column(String(20))
    country = Column(String(100))
    location_source_value = Column(String(50))
    latitude = Column(Numeric)
    longitude = Column(Numeric)

    @declared_attr
    def region_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    @declared_attr
    def region_concept(cls):
        return relationship('Concept')


class BaseLocationHistoryCdm600:
    __tablename__ = 'location_history'
    __table_args__ = {'schema': CDM_SCHEMA}

    location_history_id = Column(BigInteger, primary_key=True)

    @declared_attr
    def location_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.location.location_id'), nullable=False)

    @declared_attr
    def relationship_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    domain_id = Column(String(50), nullable=False)
    entity_id = Column(BigInteger, nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date)

    @declared_attr
    def location(cls):
        return relationship('Location')

    @declared_attr
    def relationship_type_concept(cls):
        return relationship('Concept')


class BaseProviderCdm600:
    __tablename__ = 'provider'
    __table_args__ = {'schema': CDM_SCHEMA}

    provider_id = Column(BigInteger, primary_key=True)
    provider_name = Column(String(255))
    npi = Column(String(20))
    dea = Column(String(20))

    @declared_attr
    def specialty_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def care_site_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.care_site.care_site_id'))

    year_of_birth = Column(Integer)

    @declared_attr
    def gender_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    provider_source_value = Column(String(50))
    specialty_source_value = Column(String(50))

    @declared_attr
    def specialty_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    gender_source_value = Column(String(50))

    @declared_attr
    def gender_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def care_site(cls):
        return relationship('CareSite')

    @declared_attr
    def gender_concept(cls):
        return relationship('Concept', primaryjoin='Provider.gender_concept_id == Concept.concept_id')

    @declared_attr
    def gender_source_concept(cls):
        return relationship('Concept', primaryjoin='Provider.gender_source_concept_id == Concept.concept_id')

    @declared_attr
    def specialty_concept(cls):
        return relationship('Concept', primaryjoin='Provider.specialty_concept_id == Concept.concept_id')

    @declared_attr
    def specialty_source_concept(cls):
        return relationship('Concept', primaryjoin='Provider.specialty_source_concept_id == Concept.concept_id')
