import csv
import logging
from pathlib import Path
from typing import Tuple, List, Union

from ....database import Database

logger = logging.getLogger(__name__)


class BaseClassManager:
    """
    Collects functions that interact with the Concept Class table.
    """

    def __init__(self, db: Database, cdm, custom_class_files: List[Path]):
        self.db = db
        self._cdm = cdm
        self._custom_class_files = custom_class_files

    def _get_new_custom_concept_class_ids(self) -> Tuple[List[str],List[str]]:
        # Compare custom concept_class ids and names
        # to the ones already present in the database.
        #
        # Returns 2 lists:
        # - one of concept_classes to be updated (same id, new name)
        # - one of concept_classes to be created (new id)

        logging.info('Looking for new custom class versions')

        class_ids_update = set()
        class_ids_create = set()

        for class_file in self._custom_class_files:

            with open(class_file) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    class_id = row['concept_class_id']
                    class_name = row['concept_class_name']

                    old_class_name = self._get_old_class_version(class_id)

                    # skip loading if class version already present
                    if class_name == old_class_name:
                        continue

                    if old_class_name is None:
                        class_ids_create.add(class_id)
                    else:
                        class_ids_update.add(class_id)

                    logging.info(f'Found new class version: {class_id} : '
                                 f'{old_class_name} ->  {class_name}')

        if not class_ids_update | class_ids_create:
            logging.info('No new class version found')

        return list(class_ids_update), list(class_ids_create)

    def _get_old_class_version(self, class_id: str) -> Union[bool, None]:
        # For a given custom concept_class id,
        # retrieve the name already present in the database
        # if available, otherwise None

        with self.db.session_scope() as session:
            existing_record = \
                session.query(self._cdm.ConceptClass) \
                .filter(self._cdm.ConceptClass.concept_class_id == class_id) \
                .one_or_none()
            return existing_record.concept_class_name if existing_record is not None else None

    def _drop_custom_classes(self, class_ids: List[str]) -> None:
        # Drop a list of custom concept_class ids from the database

        logging.info(f'Dropping old custom concept class versions: '
                     f'{True if class_ids else False}')

        if class_ids:
            with self.db.session_scope() as session:
                session.query(self._cdm.ConceptClass) \
                    .filter(self._cdm.ConceptClass.concept_class_id.in_(class_ids)) \
                    .delete(synchronize_session=False)

    def _load_custom_classes(self, class_ids: List[str]) -> None:
        # Load a list of new custom concept_classes to the database

        logging.info(f'Loading new custom classes: '
                     f'{True if class_ids else False}')

        if class_ids:

            with self.db.session_scope() as session:

                records = []

                for class_file in self._custom_class_files:
                    with open(class_file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        for row in reader:
                            if row['concept_class_id'] in class_ids:
                                records.append(self._cdm.ConceptClass(
                                    concept_class_id=row['concept_class_id'],
                                    concept_class_name=row['concept_class_name'],
                                    concept_class_concept_id=row['concept_class_concept_id']
                                ))

                session.add_all(records)

    def _update_custom_classes(self, class_ids: List[str]) -> None:
        # Update the name of existing custom concept_classes in the
        # database

        logging.info(f'Updating custom class names: '
                     f'{True if class_ids else False}')

        if class_ids:

            with self.db.session_scope() as session:
                for class_file in self._custom_class_files:
                    with open(class_file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        for row in reader:
                            if row['concept_class_id'] in class_ids:
                                session.query(self._cdm.ConceptClass) \
                                    .filter(self._cdm.ConceptClass.concept_class_id ==
                                            row['concept_class_id']) \
                                    .update({self._cdm.ConceptClass.concept_class_name:
                                            row['concept_class_name']})
