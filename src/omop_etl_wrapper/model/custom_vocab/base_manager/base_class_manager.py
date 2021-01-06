import csv
import logging
from pathlib import Path
from typing import List, Dict
from collections import Counter

from ....database import Database
from ....model.etl_stats import EtlTransformation, etl_stats

logger = logging.getLogger(__name__)


class BaseClassManager:
    """
    Collects functions that interact with the Concept Class table.
    """

    def __init__(self, db: Database, cdm, custom_class_files: List[Path]):
        self.db = db
        self._cdm = cdm
        self._custom_class_files = custom_class_files
        self._custom_classes_to_update = set()
        self._custom_classes_to_create = set()
        self._custom_classes_unused = set()

    def _get_custom_class_sets(self) -> None:
        # Compare custom concept_class ids and names
        # to the ones already present in the database.
        #
        # Retrieves:
        # - set of custom concept_classes to be updated in place
        #   (same id, new name)
        # - set of custom concept_classes to be created
        #   (new id)
        # - a set of obsolete custom concept_classes to be removed
        #   (id is not in use anymore)

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
                             f'{old_name} ->  {new_name}')
                if old_name is None:
                    classes_to_create.add(new_id)
                else:
                    classes_to_update.add(new_id)

        self._custom_classes_to_update = classes_to_update
        self._custom_classes_to_create = classes_to_create
        if not self._custom_classes_to_update and not self._custom_classes_to_create:
            logging.info('No new concept_class version found on disk')

        logging.info('Looking for unused custom concept_class versions')

        self._custom_classes_unused = set(classes_old.keys()) - set(classes_new.keys())

        for old_id in self._custom_classes_unused:
            logging.info(f'Found obsolete class version: {old_id}')

        if not self._custom_classes_unused:
            logging.info('No obsolete version found in database')

    def _get_old_custom_classes_from_database(self) -> Dict[str, str]:
        # Retrieve all custom concept_classes
        # (concept_class_concept_id == 0)
        # currently in database and return a dictionary {id : name}.

        class_dict = {}

        with self.db.session_scope() as session:

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

        for class_file in self._custom_class_files:

            with open(class_file) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    class_id = row['concept_class_id']
                    class_name = row['concept_class_name']
                    concept_id = row['concept_class_concept_id']

                    # quality checks
                    if not class_id:
                        raise ValueError(f'{class_file.name} may not contain an empty '
                                         f'concept_class_id')
                    if not class_name:
                        raise ValueError(f'{class_file.name} may not contain an empty '
                                         f'concept_class_name')
                    if concept_id != '0':
                        raise ValueError(f'{class_file.name} must have concept_class_concept_id '
                                         f'set to 0')
                    if class_id in class_dict.keys():
                        raise ValueError(
                            f'{class_id} has duplicates across one or multiple files')

                    class_dict[class_id] = class_name

        return class_dict

    def _drop_custom_classes(self) -> None:
        # Drop obsolete custom concept_classes from the database

        classes_to_drop = self._custom_classes_unused

        logging.info(f'Dropping unused custom concept_class versions: '
                     f'{True if classes_to_drop else False}')

        if classes_to_drop:

            transformation_metadata = EtlTransformation(name='drop_concepts')

            with self.db.session_scope(metadata=transformation_metadata) as session:
                session.query(self._cdm.ConceptClass) \
                    .filter(self._cdm.ConceptClass.concept_class_id.in_(classes_to_drop)) \
                    .delete(synchronize_session=False)

                transformation_metadata.end_now()
                etl_stats.add_transformation(transformation_metadata)

    def _load_custom_classes(self) -> None:
        # Load new custom concept_classes to the database

        classes_to_create = self._custom_classes_to_create

        logging.info(f'Loading new custom classes: '
                     f'{True if classes_to_create else False}')

        if classes_to_create:

            ignored_classes = Counter()

            for class_file in self._custom_class_files:

                transformation_metadata = EtlTransformation(name=f'load_{class_file.stem}')

                with self.db.session_scope(metadata=transformation_metadata) as session, \
                        class_file.open('r') as f_in:
                    rows = csv.DictReader(f_in, delimiter='\t')

                    records = []

                    for row in rows:
                        class_id = row['concept_class_id']

                        if class_id in classes_to_create:
                            records.append(self._cdm.ConceptClass(
                                concept_class_id=row['concept_class_id'],
                                concept_class_name=row['concept_class_name'],
                                concept_class_concept_id=row['concept_class_concept_id']
                            ))
                        elif class_id not in self._custom_classes_to_update:
                            ignored_classes.update([class_id])

                    session.add_all(records)

                    transformation_metadata.end_now()
                    etl_stats.add_transformation(transformation_metadata)

            if ignored_classes:
                logger.info(f'Skipped records with concept_class_id values that '
                            f'were already loaded under the current name: '
                            f'{ignored_classes.most_common()}')

    def _update_custom_classes(self) -> None:
        # Update the name of existing custom concept_classes in the
        # database

        classes_to_update = self._custom_classes_to_update

        logging.info(f'Updating custom class names: '
                     f'{True if classes_to_update else False}')

        if classes_to_update:

            ignored_classes = Counter()

            for class_file in self._custom_class_files:
                transformation_metadata = EtlTransformation(name=f'load_{class_file.stem}')

                with self.db.session_scope(metadata=transformation_metadata) as session, \
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
                        elif not self._custom_classes_to_create:
                            ignored_classes.update([class_id])

                    transformation_metadata.end_now()
                    etl_stats.add_transformation(transformation_metadata)

            if ignored_classes:
                logger.info(f'Skipped records with concept_class_id values that '
                            f'were already loaded under the current name: '
                            f'{ignored_classes.most_common()}')
