import csv
import logging
from pathlib import Path
from typing import Set, List

from ....database import Database
from ....model.etl_stats import EtlTransformation, etl_stats
from ....util.io import get_file_prefix

logger = logging.getLogger(__name__)


class BaseConceptManager:
    """
    Collects functions that interact with the Concept table.
    """

    def __init__(self, db: Database, cdm, custom_concept_files: List[Path]):
        self._db = db
        self._cdm = cdm
        self._custom_concept_files = custom_concept_files

    def _drop_custom_concepts(self, vocab_ids: Set[str]) -> None:
        # Drop concepts associated with a set of custom vocabulary ids
        # from the database

        logging.info(f'Dropping old custom concepts: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:

            transformation_metadata = EtlTransformation(name='drop_concepts')

            with self._db.session_scope(metadata=transformation_metadata) as session:
                session.query(self._cdm.Concept) \
                    .filter(self._cdm.Concept.vocabulary_id.in_(vocab_ids)) \
                    .delete(synchronize_session=False)

                transformation_metadata.end_now()
                etl_stats.add_transformation(transformation_metadata)

    def _load_custom_concepts(self, vocab_ids: Set[str], valid_prefixes: Set[str]) -> None:
        # Load concept_ids associated with a set of custom
        # vocabulary ids to the database

        logging.info(f'Loading new custom concept_ids: '
                     f'{True if vocab_ids else False}')

        if vocab_ids:

            unique_concepts_check = set()

            for concept_file in self._custom_concept_files:

                transformation_metadata = EtlTransformation(name=f'load_{concept_file.stem}')

                file_prefix = get_file_prefix(concept_file, 'concept')
                invalid_vocabs = set()

                with self._db.session_scope(metadata=transformation_metadata) as session, \
                        concept_file.open('r') as f_in:
                    rows = csv.DictReader(f_in, delimiter='\t')
                    records = []

                    for row in rows:
                        concept_id = row['concept_id']
                        vocabulary_id = row['vocabulary_id']

                        # quality checks
                        if int(concept_id) < 2000000000:
                            raise ValueError(
                                f'{concept_file.name} must have concept_ids starting at '
                                f'2\'000\'000\'000 (2B+ convention)')
                        if concept_id in unique_concepts_check:
                            raise ValueError(
                                f'concept {concept_id} has duplicates across one or multiple '
                                f'files')

                        if vocabulary_id in vocab_ids:
                            records.append(self._cdm.Concept(
                                concept_id=row['concept_id'],
                                concept_name=row['concept_name'],
                                domain_id=row['domain_id'],
                                vocabulary_id=row['vocabulary_id'],
                                concept_class_id=row['concept_class_id'],
                                standard_concept=row['standard_concept'],
                                concept_code=row['concept_code'],
                                valid_start_date=row['valid_start_date'],
                                valid_end_date=row['valid_end_date'],
                                invalid_reason=row['invalid_reason']
                            ))

                        # if file prefix is valid vocab_id,
                        # vocabulary_ids in file should match it.
                        # comparison is case-insensitive.
                        vocabs_lowercase = {vocab.lower() for vocab in valid_prefixes}
                        if file_prefix in vocabs_lowercase and vocabulary_id.lower() != file_prefix:
                            invalid_vocabs.add(vocabulary_id)

                    session.add_all(records)

                    transformation_metadata.end_now()
                    etl_stats.add_transformation(transformation_metadata)

                if invalid_vocabs:
                    logging.warning(f'{concept_file.name} contains vocabulary_ids '
                                    f'that do not match file prefix')
