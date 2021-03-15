Transformations
===============

.. contents::
    :local:
    :backlinks: none

Introduction
-------------
The delphyne :class:`.Wrapper` offers different ways to execute transformation scripts.
Basically, there are three options:
 - Write a Python function, returning the results as ORM objects.
 - Write a SQL query, directly inserting into the target table.
 - Write a query using the SQLAlchemy expression language.

For both options the :class:`.Wrapper` has built-in methods that coordinate the execution.
These methods, detailed below, use SQLAlchemy to handle database operations and log the execution statistics.
This provides a standardised interface independent of whether you are using Python or SQL for the transformation logic.

It is good practice to create a separate transformation file (either Python or SQL) for each table to table
transformation.
This improves maintainability and allows for delphyne creating a detailed mapping log.
In addition, the advised convention for the transformation file naming is `<source_table>_to_<target_table>.py/sql`.

Plain Python
-------------
The plain Python approach allows the use of 'simple' Python to write transformations.
This function takes the wrapper as input and produces OMOP records as SQLAlchemy ORM objects.
It can return these objects in two different ways (see below).
The convention is to put the transformation scripts in the `src/main/python/transformation` folder.
`See delphyne-template for examples. <https://github.com/thehyve/delphyne-template/tree/master/src/main/python/transformation>`_

There are two ways of execution the transformation, each requiring a different return type.
The first mode, used by :meth:`.Wrapper.execute_transformation`.
This expects the transformation function to return the full list of ORM objects at once.
All returned records are committed to the database at the same time.

.. code-block:: python

    def my_transformation(wrapper):
        # ...read source data...
        records_to_insert = []
        for row in source:
            # ...transformation logic...
            omop_record = wrapper.cdm.Observation(
                person_id=...,
                observation_concept_id=...,
                ...
            )
            records_to_insert.append(records_to_insert)
        return records_to_insert

While this is straightforward, it does require to store all records in memory.
For large tables there is a 'batch' mode :meth:`.Wrapper.execute_batch_transformation`.
This expects the transformation function to yield the ORM objects one at a time.
The wrapper will commits the yielded records to the database in batches of given size.

.. code-block:: python

    def my_batch_transformation(wrapper):
        # ...read source data...
        for row in source:
            # ...transformation logic...
            omop_record = wrapper.cdm.Observation(
                person_id=...,
                observation_concept_id=...,
                ...
            )
            yield omop_record

In the wrapper these two transformations can be called like this:

.. code-block:: python

    self.execute_transformation(my_transformation)
    self.execute_batch_transformation(my_batch_transformation)



Raw SQL
-------------
SQL queries can easily be executed with the wrapper.
In case of just executing a simple query, the method :meth:`.Wrapper.execute_sql_query` is used.
If the SQL query is saved in a file, the method :meth:`.Wrapper.execute_sql_file` is used.

The SQL query should handle the insertion of records.
The easiest way to create a transformations with sql is by following the template given here.
If you have defined your transformation in Rabbit-in-a-Hat, then
you can directly export `a SQL skeleton in this format <http://ohdsi.github.io/WhiteRabbit/RabbitInAHat.html#generating_a_sql_skeleton_(v090)>`_.
The convention is to put these transformation scripts in the `src/main/sql` folder.
`See delphyne-template for examples. <https://github.com/thehyve/delphyne-template/tree/master/src/main/sql>`_

.. code-block:: sql

    INSERT INTO @cdm_schema.<target_table> (
     <target_column1>,
     <target_column2>,
     ...
    )
    SELECT
     <source_column1>,
     <source_column2>,
     ...
    FROM @source_schema.<source_table>


SQLAlchemy query
-------------
Instead of writing plain SQL query, the query can also be written using SQLAlchemy expressions.
SQLAlchemy translates the expressions into SQL.
This has the advantage that it can be compiled to any SQL dialect and makes the query agnostic of the used
Relational Database Management System (RDBMS).

**TBC**
