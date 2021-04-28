Table constraints
=================

.. contents::
    :local:
    :backlinks: none

CDM tables created with delphyne will by default have all indexes, PKs, FKs and other constraints applied.
The :class:`.ConstraintManager` class allows you to drop or add these on the level of your choice.

Naming conventions
------------------

Unless names are explicitly provided in your CDM table definitions, constraints and indexes will be named according to
a set of conventions. These conventions ensure that the names will be the same, regardless of the DBMS.
delphyne applies the following naming conventions
(see the `SQLAlchemy documentation on naming conventions
<https://docs.sqlalchemy.org/en/14/core/constraints.html#configuring-a-naming-convention-for-a-metadata-collection>`_
for an explanation of the keywords):

.. code-block:: python

    NAMING_CONVENTION = {
        "ix": "ix_%(table_name)s_%(column_0_N_name)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s"
    }


Drop/add constraints
--------------------
Dropping or adding constraints can be done at any time during the data transformation process.
Depending on the aim, this can be applied to a single constraint, up until the full set of
constraints across all tables.

.. note::
   Dropping indexes/constraints does not cascade. This means they can only be dropped if it doesn't violate other
   database relationships, and only added if the prerequisite objects are present.

Individual constraints
^^^^^^^^^^^^^^^^^^^^^^

Dropping or adding a single constraint/index is done by providing the name:

.. code-block:: python

    # Drop index
    wrapper.db.constraint_manager.drop_constraint_or_index('ix_measurement_person_id')
    # Add index
    wrapper.db.constraint_manager.add_constraint_or_index('ix_measurement_person_id')

When dropping, the provided name should reflect what is currently active in the database. When adding, the name should
exist in your CDM model (either via naming convention, or an explicitly defined name). Trying to drop a
constraint/index that is not currently active, or adding one that does not exist in your CDM tables
definition, will raise a ``KeyError``.

.. warning::
   While delphyne can be used with CDMs created with the
   `official DDL scripts <https://github.com/OHDSI/CommonDataModel/>`_, if dropped constraints are later added again,
   the names will likely be different, as they will be determined by the naming conventions.

Multiple constraints
^^^^^^^^^^^^^^^^^^^^

Constraints and/or indexes can also be dropped or added on table-level. From smallest to largest, the levels are:
single table, cdm (non-vocabulary) tables, and the complete set of tables in your model (including vocabularies).

.. code-block:: python

    # Drop all indexes/constraints on the observation table
    wrapper.db.constraint_manager.drop_table_constraints('observation')
    # Drop all indexes/constraints on all non-vocabulary tables
    wrapper.db.constraint_manager.drop_cdm_constraints()
    # Drop all indexes/constraints on all tables
    wrapper.db.constraint_manager.drop_all_constraints()

All of these methods have arguments that allow you to specify the categories of the objects that should be added
or dropped: PKs, indexes and constraints (includes FKs, check and unique constraints).
The default behavior is to include all objects.

Dropping behavior ignores the CDM model, i.e. it will look at the objects currently active on the respective tables,
and drops all that match the specified categories (e.g. indexes, PKs).

Adding behavior on the other hand, will look at the objects specified in the CDM model.
It will, however, only add objects that are not already active in the database.
Any index/constraint that is already active, under the same or a different name, will therefore be skipped.

E.g. the following drops all constraints (but not PKs and indexes) on all non-vocabulary tables:

.. code-block:: python

    wrapper.db.constraint_manager.drop_cdm_constraints(drop_constraint=True,
                                                       drop_pk=False,
                                                       drop_index=False)


Drop or add methods can only be run successfully if the action does not cause conflicts.
E.g. dropping a PK will not be possible if other FKs still depend on it. The default behavior in case an action
cannot be performed is to raise an exception.
Any constraints/indexes that were already dropped or added as part of the method will not be rolled back.
To only log these occurrences without raising an exception, and continue to try to add/drop remaining
objects (if any), the ``errors='ignore'`` argument can be provided.

Use cases
---------

Before running transformations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To increase insert performance, it is highly recommended to remove all constraints and indexes on non-vocabulary tables
before running any transformations.
This can be done by calling :meth:`~.ConstraintManager.drop_cdm_constraints()`.
After all transformations have completed, they can be restored again: :meth:`~.ConstraintManager.add_cdm_constraints()`.

In between transformations
^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes it's useful to add a PK or index to a populated CDM table, before starting another transformation.
If, for example, the transformation that populates the CONDITION_OCCURRENCE table uses an inner join on the person_id
field of the PERSON table, it would benefit from having the PK already being present on the PERSON table.

The same principle applies when transformations require 'lookups' in already populated CDM tables.
