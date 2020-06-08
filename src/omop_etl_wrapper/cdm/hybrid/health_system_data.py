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

from omop_etl_wrapper import Base
from sqlalchemy import BigInteger, Column, Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship

# coding: utf-8
from .._defaults import VOCAB_SCHEMA, CDM_SCHEMA


class CareSite(Base):
    __tablename__ = 'care_site'
    __table_args__ = {'schema': CDM_SCHEMA}

    care_site_id = Column(BigInteger, primary_key=True)
    care_site_name = Column(String(255))
    place_of_service_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    location_id = Column(ForeignKey(f'{CDM_SCHEMA}.location.location_id'))
    care_site_source_value = Column(String(50))
    place_of_service_source_value = Column(String(50))

    location = relationship('Location')
    place_of_service_concept = relationship('Concept')


class Location(Base):
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
    region_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'))

    region_concept = relationship('Concept')


class LocationHistory(Base):
    __tablename__ = 'location_history'
    __table_args__ = {'schema': CDM_SCHEMA}

    location_history_id = Column(BigInteger, primary_key=True)
    location_id = Column(ForeignKey(f'{CDM_SCHEMA}.location.location_id'), nullable=False)
    relationship_type_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    domain_id = Column(String(50), nullable=False)
    entity_id = Column(BigInteger, nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date)

    location = relationship('Location')
    relationship_type_concept = relationship('Concept')


class Provider(Base):
    __tablename__ = 'provider'
    __table_args__ = {'schema': CDM_SCHEMA}

    provider_id = Column(BigInteger, primary_key=True)
    provider_name = Column(String(255))
    npi = Column(String(20))
    dea = Column(String(20))
    specialty_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    care_site_id = Column(ForeignKey(f'{CDM_SCHEMA}.care_site.care_site_id'))
    year_of_birth = Column(Integer)
    gender_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    provider_source_value = Column(String(50))
    specialty_source_value = Column(String(50))
    specialty_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)
    gender_source_value = Column(String(50))
    gender_source_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), nullable=False)

    care_site = relationship('CareSite')
    gender_concept = relationship('Concept', primaryjoin='Provider.gender_concept_id == Concept.concept_id')
    gender_source_concept = relationship('Concept', primaryjoin='Provider.gender_source_concept_id == Concept.concept_id')
    specialty_concept = relationship('Concept', primaryjoin='Provider.specialty_concept_id == Concept.concept_id')
    specialty_source_concept = relationship('Concept', primaryjoin='Provider.specialty_source_concept_id == Concept.concept_id')
