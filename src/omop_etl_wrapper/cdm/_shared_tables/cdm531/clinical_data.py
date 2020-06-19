from sqlalchemy import Column, Date, DateTime, ForeignKey, String
from sqlalchemy.orm import relationship

from ..._schema_placeholders import VOCAB_SCHEMA, CDM_SCHEMA
from .... import Base

metadata = Base.metadata


class Death(Base):
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
