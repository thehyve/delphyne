"""OMOP CDM 6.0.0 health system tables."""

from sqlalchemy import (BigInteger, Column, Date, ForeignKey, Integer,
                        Numeric, String)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

from ..schema_placeholders import VOCAB_SCHEMA, CDM_SCHEMA


class BaseCareSiteCdm600:
    __tablename__ = 'care_site'
    __table_args__ = {'schema': CDM_SCHEMA}

    @declared_attr
    def care_site_id(cls):
        return Column(BigInteger, primary_key=True)

    @declared_attr
    def care_site_name(cls):
        return Column(String(255))

    @declared_attr
    def place_of_service_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def location_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.location.location_id'))

    @declared_attr
    def care_site_source_value(cls):
        return Column(String(50))

    @declared_attr
    def place_of_service_source_value(cls):
        return Column(String(50))

    @declared_attr
    def location(cls):
        return relationship('Location')

    @declared_attr
    def place_of_service_concept(cls):
        return relationship('Concept')


class BaseLocationCdm600:
    __tablename__ = 'location'
    __table_args__ = {'schema': CDM_SCHEMA}

    @declared_attr
    def location_id(cls):
        return Column(BigInteger, primary_key=True)

    @declared_attr
    def address_1(cls):
        return Column(String(50))

    @declared_attr
    def address_2(cls):
        return Column(String(50))

    @declared_attr
    def city(cls):
        return Column(String(50))

    @declared_attr
    def state(cls):
        return Column(String(2))

    @declared_attr
    def zip(cls):
        return Column(String(9))

    @declared_attr
    def county(cls):
        return Column(String(20))

    @declared_attr
    def country(cls):
        return Column(String(100))

    @declared_attr
    def location_source_value(cls):
        return Column(String(50))

    @declared_attr
    def latitude(cls):
        return Column(Numeric)

    @declared_attr
    def longitude(cls):
        return Column(Numeric)

    @declared_attr
    def region_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    @declared_attr
    def region_concept(cls):
        return relationship('Concept')


class BaseLocationHistoryCdm600:
    __tablename__ = 'location_history'
    __table_args__ = {'schema': CDM_SCHEMA}

    @declared_attr
    def location_history_id(cls):
        return Column(BigInteger, primary_key=True)

    @declared_attr
    def location_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.location.location_id'), nullable=False)

    @declared_attr
    def relationship_type_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def domain_id(cls):
        return Column(String(50), nullable=False)

    @declared_attr
    def entity_id(cls):
        return Column(BigInteger, nullable=False)

    @declared_attr
    def start_date(cls):
        return Column(Date, nullable=False)

    @declared_attr
    def end_date(cls):
        return Column(Date)

    @declared_attr
    def location(cls):
        return relationship('Location')

    @declared_attr
    def relationship_type_concept(cls):
        return relationship('Concept')


class BaseProviderCdm600:
    __tablename__ = 'provider'
    __table_args__ = {'schema': CDM_SCHEMA}

    @declared_attr
    def provider_id(cls):
        return Column(BigInteger, primary_key=True)

    @declared_attr
    def provider_name(cls):
        return Column(String(255))

    @declared_attr
    def npi(cls):
        return Column(String(20))

    @declared_attr
    def dea(cls):
        return Column(String(20))

    @declared_attr
    def specialty_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def care_site_id(cls):
        return Column(ForeignKey(f'{CDM_SCHEMA}.care_site.care_site_id'))

    @declared_attr
    def year_of_birth(cls):
        return Column(Integer)

    @declared_attr
    def gender_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def provider_source_value(cls):
        return Column(String(50))

    @declared_attr
    def specialty_source_value(cls):
        return Column(String(50))

    @declared_attr
    def specialty_source_concept_id(cls):
        return Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    @declared_attr
    def gender_source_value(cls):
        return Column(String(50))

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
