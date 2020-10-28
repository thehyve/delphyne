from sqlalchemy import (Column, Date, DateTime, Integer, String, Text)

from .._schema_placeholders import CDM_SCHEMA


class BaseCdmSourceCdm531:
    __tablename__ = 'cdm_source'
    __table_args__ = {'schema': CDM_SCHEMA}

    cdm_source_name = Column(String(255), primary_key=True, nullable=False)
    cdm_source_abbreviation = Column(String(25))
    cdm_holder = Column(String(255))
    source_description = Column(Text)
    source_documentation_reference = Column(String(255))
    cdm_etl_reference = Column(String(255))
    source_release_date = Column(Date)
    cdm_release_date = Column(Date)
    cdm_version = Column(String(10))
    vocabulary_version = Column(String(20))


class BaseMetadataCdm531:
    __tablename__ = 'metadata'
    __table_args__ = {'schema': CDM_SCHEMA}

    metadata_concept_id = Column(Integer, primary_key=True, nullable=False, index=True)
    metadata_type_concept_id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(250), primary_key=True, nullable=False)
    value_as_string = Column(Text)
    value_as_concept_id = Column(Integer)
    metadata_date = Column(Date)
    metadata_datetime = Column(DateTime)
