"""concept_class vocabulary table operations."""

import csv
import logging
from pathlib import Path
from typing import List, Dict

from ....database import Database

logger = logging.getLogger(__name__)


class ClassManager:
    """
    Collection of concept_class vocabulary table functions.

    Parameters
    ----------
    db : Database
        Database instance to interact with.
    cdm : module
        Module containing all CDM table definitions.
    custom_class_files : list of pathlib.Path
        Collection of files containing custom concept_class data.
    """

    def __init__(self, db: Database, cdm, custom_class_files: List[Path]):
        self._db = db
        self._cdm = cdm
        self.custom_class_files = custom_class_files
        self.custom_classes_to_update = set()
        self.custom_classes_to_create = set()
        self.custom_classes_unused = set()

        if not self.custom_class_files:
            logger.info('No concept_class.tsv file found')

    def get_custom_class_sets(self) -> None:
        """
        Compare custom concept_classes between files and database.

        Detects differences in both concept_class_id and
        concept_class_name.

        Retrieves:
        - a set of custom concept_classes to be updated in place
          (same id, new name)
        - a set of custom concept_classes to be created
          (new id)
        - a set of obsolete custom concept_classes to be removed
          (id not in use anymore)

        """
        logging.info('Looking for new custom class versions')

        classes_old = self._get_old_custom_classes_from_database()
        classes_new = self._get_new_custom_classes_from_disk()

        classes_to_create = set()
        classes_to_update = set()

        for new_id, new_name in classes_new.items():
            old_name = classes_old.get(new_id, None)

            # skip version if already present in database
            if old_name == new_name:
                continue
            else:
                # if class didn't exist before, old_name is None
                logging.info(f'Found new concept_class version: {new_id} : '
                             f'{old_name} -> {new_name}')
                if old_name is None:
                    classes_to_create.add(new_id)
                else:
                    classes_to_update.add(new_id)

        self.custom_classes_to_update = classes_to_update
        self.custom_classes_to_create = classes_to_create
        if not self.custom_classes_to_update and not self.custom_classes_to_create:
            logging.info('No new concept_class version found on disk')

        logging.info('Looking for unused custom concept_class versions')

        self.custom_classes_unused = classes_old.keys() - classes_new.keys()

        for old_id in self.custom_classes_unused:
            logging.info(f'Found obsolete concept_class version: {old_id}')

        if not self.custom_classes_unused:
            logging.info('No obsolete version found in database')

    def _get_old_custom_classes_from_database(self) -> Dict[str, str]:
        # Retrieve all custom concept_classes
        # (concept_class_concept_id == 0)
        # currently in database and return a dictionary {id : name}.

        class_dict = {}

        with self._db.session_scope() as session:

            records = session.query(self._cdm.ConceptClass) \
                .filter(self._cdm.ConceptClass.concept_class_concept_id == 0) \
                .all()

            for record in records:
                class_dict[record.concept_class_id] = record.concept_class_name

        return class_dict

    def _get_new_custom_classes_from_disk(self) -> Dict[str, str]:
        # Retrieve all user-provided custom concept_classes from disk
        # and return a dictionary {id : name}.

        class_dict = {}
        errors = set()
        files_with_errors = set()

        for class_file in self.custom_class_files:

            file_errors = False

            with open(class_file) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    class_id = row['concept_class_id']
                    class_name = row['concept_class_name']
                    concept_id = row['concept_class_concept_id']

                    # quality checks
                    if not class_id:
                        errors.add(f'{class_file.name} may not contain an empty '
                                   f'concept_class_id')
                        file_errors = True
                    if not class_name:
                        errors.add(f'{class_file.name} may not contain an empty '
                                   f'concept_class_name')
                        file_errors = True
                    if concept_id != '0':
                        errors.add(f'{class_file.name} may not contain concept_class_concept_id'
                                   f' other than 0')
                        file_errors = True
                    if class_id in class_dict:
                        errors.add(f'concept class {class_id} is duplicated across one or '
                                   f'multiple files')
                        file_errors = True

                    class_dict[class_id] = class_name

            if file_errors:
                files_with_errors.add(class_file.name)

        if files_with_errors:
            for error in sorted(errors):
                logger.error(error)
            files_with_errors = sorted(files_with_errors)
            raise ValueError(f'Concept class files {files_with_errors} contain invalid values')

        return class_dict

    def drop_custom_classes(self) -> None:
        """Drop obsolete custom concept_classes from the database."""
        classes_to_drop = self.custom_classes_unused

        logging.info(f'Dropping unused custom concept_class versions: '
                     f'{True if classes_to_drop else False}')

        if not classes_to_drop:
            return

        with self._db.tracked_session_scope(name='drop_classes') as (session, _):
            session.query(self._cdm.ConceptClass) \
                .filter(self._cdm.ConceptClass.concept_class_id.in_(classes_to_drop)) \
                .delete(synchronize_session=False)

    def load_custom_classes(self) -> None:
        """Load new custom concept_class records to the database."""
        classes_to_create = self.custom_classes_to_create

        logging.info(f'Loading new custom classes: '
                     f'{True if classes_to_create else False}')

        if not classes_to_create:
            return

        ignored_classes = set()

        for class_file in self.custom_class_files:
            with self._db.tracked_session_scope(name=f'load_{class_file.stem}') as (session, _), \
                    class_file.open('r') as f_in:
                rows = csv.DictReader(f_in, delimiter='\t')

                for row in rows:
                    class_id = row['concept_class_id']

                    if class_id in classes_to_create:
                        session.add(self._cdm.ConceptClass(
                            concept_class_id=row['concept_class_id'],
                            concept_class_name=row['concept_class_name'],
                            concept_class_concept_id=row['concept_class_concept_id']
                        ))
                    elif class_id not in self.custom_classes_to_update:
                        ignored_classes.add(class_id)

        if ignored_classes:
            logger.info(f'Skipped records with concept_class_id values that '
                        f'were already loaded under the current name: '
                        f'{ignored_classes}')

    def update_custom_classes(self) -> None:
        """
        Update existing custom concept_classes in the database.

        concept_class_name is updated in place.

        """
        classes_to_update = self.custom_classes_to_update

        logging.info(f'Updating custom class names: '
                     f'{True if classes_to_update else False}')

        if not classes_to_update:
            return

        ignored_classes = set()

        for class_file in self.custom_class_files:
            with self._db.tracked_session_scope(name=f'load_{class_file.stem}') as (session, _), \
                    class_file.open('r') as f_in:
                rows = csv.DictReader(f_in, delimiter='\t')

                for row in rows:
                    class_id = row['concept_class_id']

                    if class_id in classes_to_update:
                        session.query(self._cdm.ConceptClass) \
                            .filter(self._cdm.ConceptClass.concept_class_id
                                    == row['concept_class_id']) \
                            .update({self._cdm.ConceptClass.concept_class_name:
                                    row['concept_class_name']})

                    # this check has already been performed in the
                    # _load_custom_classes transformation, unless
                    # there were no classes new class_ids to add
                    elif not self.custom_classes_to_create:
                        ignored_classes.add(class_id)

        if ignored_classes:
            logger.info(f'Skipped records with concept_class_id values that '
                        f'were already loaded under the current name: '
                        f'{ignored_classes}')
