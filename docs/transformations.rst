Transformations
===============

.. contents::
    :local:
    :backlinks: none

General intro
-------------
By extending the :mod:`~src.delphyne.wrapper.Wrapper` a number of transformation functions are available.
Basically, there are two options:
 - Write a python function, returning the results as ORM objects.
 - Write a SQL query, directly inserting into the target table.

For both options the :mod:`~src.delphyne.wrapper.Wrapper` has build-in methods that coordinate the execution.
These methods, detailed below, use SqlAlchemy to handle database operations and log the execution statistics.
This provides a standardised interface independent of whether you are using Python or Sql for the transformation logic.

ORM
---
The ORM approach allows the use of pure python to write transformations.
This function takes the wrapper as input and produces OMOP records as SqlAlchemy ORM objects.
It can return these objects in two different ways (see below).
While records from multiple different OMOP tables can be created in the same function,
 it is good practice to create a separate file for each table to table transformation.
See delphyne-template for examples.

There are two ways of execution the transformation, each requiring a different return type.
The first mode, used by :mod:`~src.delphyne.wrapper.Wrapper.execute_transformation`.
This expects the transformation function to return the full list of ORM objects at once.
All returned records are committed to the database at the same time.

While this is straightforward, it does require to store all records in memory.
For large tables there is a 'batch' mode :mod:`~src.delphyne.wrapper.Wrapper.execute_batch_transformation`.
This expects the transformation function to yield the ORM objects one at a time.
The wrapper will commits the yielded records to the database in batches of given size.

The pseudo-code for a transformation is given below, with the only difference between the two
execution modes being the way the omop records are returned.
```python
def my_transformation(wrapper):
    # read source data
    for row in source:
        # transformation logic...
        omop_record = OmopTable(  # e.g. Person, Observation, ...
            person_id=...,
            concept_id=...,
            ...
        )
        # yield OR store in list
    # return list (if not yielding)
```


Raw SQL
-------------
SQL queries can easily be executed with through the wrapper.
In case of just executing a simple query, the method :mod:`~src.delphyne.wrapper.Wrapper.execute_sql_query` is used.
If the sql query is saved in a file, the method :mod:`~src.delphyne.wrapper.Wrapper.execute_sql_file` is used.

The sql query should handle the insertion of records.
The easiest way to create a transformations with sql is by following the template given here.
If you have defined your transformation in Rabbit-in-a-Hat, then
 you can directly export `a sql skeleton in this format <http://ohdsi.github.io/WhiteRabbit/RabbitInAHat.html#generating_a_sql_skeleton_(v090)>`_.

```sql
INSERT INTO @cdmSchma.<target_table> (
 <target_column1>,
 <target_column2>,
 ...
)
SELECT
 <source_column1>,
 <source_column2>,
 ...
FROM @sourceSchema.<source_table>
```
