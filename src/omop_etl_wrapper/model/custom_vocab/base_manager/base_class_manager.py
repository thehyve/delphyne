import csv
import logging
from pathlib import Path
from typing import List, Union

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
        self._class_ids_update = set()
        self._class_ids_create = set()
        self._class_ids_all = set()

    def _get_new_custom_concept_class_ids(self) -> None:
        # Compare custom concept_class ids and names
        # to the ones already present in the database.
        #
        # Retrieves:
        # - set of custom concept_classes to be updated
        #   (same id, new name)
        # - set of custom concept_classes to be created
        #   (new id)
        # - set of all user-provided custom concept_classes

        logging.info('Looking for new custom class versions')

        for class_file in self._custom_class_files:

            with open(class_file) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    class_id = row['concept_class_id']
                    class_name = row['concept_class_name']

                    self._class_ids_all.add(class_id)

                    old_class_name = self._get_old_class_version(class_id)

                    # skip loading if class version already present
                    if class_name == old_class_name:
                        continue

                    if old_class_name is None:
                        self._class_ids_create.add(class_id)
                    else:
                        self._class_ids_update.add(class_id)

                    logging.info(f'Found new class version: {class_id} : '
                                 f'{old_class_name} ->  {class_name}')

        if not self._class_ids_update | self._class_ids_create:
            logging.info('No new class version found')

    def _get_old_class_version(self, class_id: str) -> Union[bool, None]:
        # For a given custom concept_class id, retrieve the name
        # already present in the database if available, otherwise None

        with self.db.session_scope() as session:
            existing_record = \
                session.query(self._cdm.ConceptClass) \
                .filter(self._cdm.ConceptClass.concept_class_id == class_id) \
                .one_or_none()
            return existing_record.concept_class_name if existing_record is not None else None

    def _load_custom_classes(self) -> None:
        # Load new custom concept_classes to the database

        logging.info(f'Loading new custom classes: '
                     f'{True if self._class_ids_create else False}')

        if self._class_ids_create:

            with self.db.session_scope() as session:
                records = []
                for class_file in self._custom_class_files:
                    with open(class_file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        for row in reader:
                            if row['concept_class_id'] in self._class_ids_create:
                                records.append(self._cdm.ConceptClass(
                                    concept_class_id=row['concept_class_id'],
                                    concept_class_name=row['concept_class_name'],
                                    concept_class_concept_id=row['concept_class_concept_id']
                                ))

                session.add_all(records)

    def _update_custom_classes(self) -> None:
        # Update the name of existing custom concept_classes in the
        # database

        logging.info(f'Updating custom class names: '
                     f'{True if self._class_ids_update else False}')

        if self._class_ids_update:

            with self.db.session_scope() as session:
                for class_file in self._custom_class_files:
                    with open(class_file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        for row in reader:
                            if row['concept_class_id'] in self._class_ids_update:
                                session.query(self._cdm.ConceptClass) \
                                    .filter(self._cdm.ConceptClass.concept_class_id ==
                                            row['concept_class_id']) \
                                    .update({self._cdm.ConceptClass.concept_class_name:
                                            row['concept_class_name']})

    def _drop_unused_custom_classes(self) -> None:
        # Drop obsolete custom concept classes from the database;
        # these are assumed to be all classes in the database with
        # concept_id == 0 minus all user-provided custom classes.

        logging.info(f'Checking for obsolete custom concept class versions')

        if self._class_ids_all:
            with self.db.session_scope() as session:

                query_base = session.query(self._cdm.ConceptClass) \
                    .filter(self._cdm.ConceptClass.concept_class_concept_id == 0) \
                    .filter(self._cdm.ConceptClass.concept_class_id.notin_(self._class_ids_all))

                record = query_base.one_or_none()
                if record:
                    logging.info(f'Dropping unused custom class: {record.concept_class_id}')
                    query_base.delete(synchronize_session=False)
