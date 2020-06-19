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

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, String, Date, Numeric
from sqlalchemy.orm import relationship

from .._schema_placeholders import VOCAB_SCHEMA, CDM_SCHEMA
from ... import Base


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
    person = relationship('Person')


class EpisodeEvent(Base):
    __tablename__ = 'episode_event'
    __table_args__ = {'schema': CDM_SCHEMA}

    episode_id = Column(BigInteger, primary_key=True, nullable=False, index=True)
    event_id = Column(BigInteger, primary_key=True, nullable=False)
    event_field_concept_id = Column(ForeignKey(f'{VOCAB_SCHEMA}.concept.concept_id'), primary_key=True, nullable=False,
                                    index=True)

    event_field_concept = relationship('Concept')
