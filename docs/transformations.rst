Transformations
===============

.. contents::
    :local:
    :backlinks: none


Introduction
------------
The delphyne :class:`.Wrapper` offers different ways to execute transformation scripts.
Basically, there are three options:

 - Write a Python function, returning the results as `SQLAlchemy ORM objects <https://docs.sqlalchemy.org/en/14/orm>`_.
 - Write a SQL query, directly inserting into the target table.
 - Write a Python function, creating a query using the `SQLAlchemy expression language <https://docs.sqlalchemy.org/en/14/core>`_.

For all of these options, the :class:`.Wrapper` has built-in methods that coordinate the execution.
These methods, detailed below, use SQLAlchemy to handle database operations and log the execution statistics.
This provides a standardised interface independent of whether you are using Python or SQL for the transformation logic.

It is good practice to create a separate transformation file (either Python or SQL) for each table to table
transformation.
This improves maintainability and allows for delphyne creating a detailed mapping log.
In addition, the advised convention for the transformation file naming is ``<source_table>_to_<target_table>.py/sql``.


SQLAlchemy ORM
--------------
The SQLAlchemy ORM approach allows the use of 'simple' Python to write transformations.
This function takes the wrapper as input and produces OMOP records as SQLAlchemy ORM objects.
The convention is to put the transformation scripts in the `src/main/python/transformation` folder.
`See delphyne-template for examples. <https://github.com/thehyve/delphyne-template/tree/master/src/main/python/transformation>`_

There are two ways of executing the transformation, each requiring a different return type.
:meth:`.Wrapper.execute_transformation`, expects the transformation function to return a full list of ORM objects at once.
All returned records are committed to the database at the same time.

.. code-block:: python

    def my_transformation(wrapper):
        # ...read source data...
        records_to_insert = []
        for row in source:
            # ...transformation logic...
            record = wrapper.cdm.Observation(
                person_id=...,
                observation_concept_id=...,
                ...
            )
            records_to_insert.append(record)
        return records_to_insert

While this is straightforward, it does require to store all output records in memory.
For large tables there is a 'batch' mode :meth:`.Wrapper.execute_batch_transformation`.
This expects the transformation function to yield the ORM objects one at a time.
The wrapper commits the yielded records to the database in batches of 10_000 records by default.

.. code-block:: python

    def my_batch_transformation(wrapper):
        # ...read source data...
        for row in source:
            # ...transformation logic...
            record = wrapper.cdm.Observation(
                person_id=...,
                observation_concept_id=...,
                ...
            )
            yield record

Inside a wrapper method, these two transformations can be called like this.
Specifying the batch size is optional.

.. code-block:: python
    def run(self):
        ...
        self.execute_transformation(my_transformation)
        self.execute_batch_transformation(my_batch_transformation, batch_size=10000)


Raw SQL
-------
SQL queries can easily be executed with the wrapper.
In case of just executing a simple query, the method :meth:`.Wrapper.execute_sql_query` is used.
If the SQL query is saved in a file, the method :meth:`.Wrapper.execute_sql_file` is used.

The SQL query should handle the insertion of records.
The easiest way to create a transformations with SQL is by following the template given here.
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

Executing the file is then done by adding the following line to a wrapper method.
Only the filename has to be provided, delphyne will look for the file in the `src/main/sql` folder.

.. code-block:: python
        self.execute_sql_file('my_file.sql')


SQLAlchemy query
----------------
Instead of writing plain SQL query, the query can also be written using SQLAlchemy expressions.
SQLAlchemy translates the expressions into SQL.
This has the advantage that it can be compiled to any SQL dialect and makes the query agnostic of the used
Relational Database Management System (RDBMS).


.. code-block:: python

    from sqlalchemy import select

    def my_sql_transformation(wrapper):
    
        source_table = wrapper.cdm.<source_table>.__table__
        target_table = wrapper.cdm.<target_table>.__table__

        sel = select([
            source_table.columns['source_column_1'],
            source_table.columns['source_column_2'],
            ...
            ])\
            .select_from(source_table)

        ins = target_table.insert().from_select(sel.columns, sel)
        
        return ins
        
In case the source table is not part of the CDM schema, you can obtain it with the following method, which leverages SQLAlchemy's ability to create reflected table objects from the database itself:

.. code-block:: python
        source_table = wrapper.get_table(schema='my_source_schema', table_name='my_source_table')
        
Inside a wrapper method, the transformations can be called like this, similar to ORM transformations.

.. code-block:: python
    def run(self):
        ...
        self.execute_sql_transformation(my_sql_transformation)
