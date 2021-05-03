Source data
===========

.. contents::
    :local:
    :backlinks: none
    :depth: 2

Converting your dataset to the OMOP CDM will require processing of the source data.
Depending on the type of :ref:`transformations <transformations:Transformations>` you use, this can be either reading
the source files directly in Python, or by referencing an already loaded source data table in your database.
While you are free to implement the loading of source data into a data structure in any way you prefer,
some generally used formats are already built in.

Read directly from file
-----------------------
Source data files can be read directly to be used in :ref:`transformations:SQLAlchemy ORM` style transformations.
This requires a `source data path <TODO>`_ to be set in the main config file,
and the presence of a `source_config <TODO>`_.

To use a file in a transformation, first define the source by providing the file name:

.. code-block:: python

    source = wrapper.source_data.get_source_file('source_table.csv')

The resulting object can then be used to access the file contents in various ways, depending on the file type.

Delimited text files
--------------------
Delimited text files (e.g. csv) can be used in transformations in several ways.
Any file-specific information (e.g. encoding, delimiter) needed to read the file should be provided in the
source_config.

Python dictionaries
^^^^^^^^^^^^^^^^^^^
The simplest way of using delimited text files is to load them as Python dictionaries, as implemented by the
`csv library <https://docs.python.org/3/library/csv.html#csv.DictReader>`_.
This will allow you to loop through the file's records, each row represented as a dictionary.
Keys are static and determined by the file header, while the values reflect the contents of a particular row.
All values will be loaded as strings.

Full list of records
""""""""""""""""""""
To load the full contents of a delimited text file upfront, use the :meth:`~.SourceFile.get_csv_as_list_of_dicts()`
method. This will retrieve the rows as a list of dictionaries, allowing you access to all the rows in any order.
Naturally, this requires that the full contents of the file will fit into memory.

Generator
"""""""""
Alternatively, the contents of the file can be accessed through a generator object
with :meth:`~.SourceFile.get_csv_as_generator_of_dicts()`.
Looping through the rows in this way allows you to access only one row at a time, but keeps memory use very low.

pandas dataframe
^^^^^^^^^^^^^^^^
Delimited text files can also be loaded as a `pandas <https://pandas.pydata.org/>`_ dataframe
using :meth:`~.SourceFile.get_csv_as_df()`.
If pandas dtypes are provided in the source_config, they can be applied to the dataframe. If not, or if you don't want
to apply them directly, ``apply_dtypes`` can be set to ``False``:

.. code-block:: python

    source = wrapper.source_data.get_source_file('source_table.csv')
    df = source.get_csv_as_df(apply_dtypes=False)


If you do want to apply the dtypes from your source_config, but cannot apply them straight away (e.g. you first need to
do some preprocessing of the data), you can also use the :meth:`~.SourceFile.apply_dtypes()` method directly:

.. code-block:: python

    source = wrapper.source_data.get_source_file('source_table.csv')
    df = source.get_csv_as_df(apply_dtypes=False)
    # Do some preprocessing on df here
    df = source.apply_dtypes(df)

Caching
^^^^^^^
If the same source file is used in different transformations, it could be useful to keep the contents of the file in
memory, to make sure they don't have to be read multiple times.
The simplest way to indicate a data structure should be kept in memory after a transformation is completed, is by adding
the ``cache=true`` argument to a data retrieval method:

.. code-block:: python

    source = wrapper.source_data.get_source_file('source_table.csv')
    # Caching applied to a dataframe
    df = source.get_csv_as_df(apply_dtypes=True, cache=True)
    # Or to a list of dictionaries
    records = source.get_csv_as_list_of_dicts(cache=True)

For dataframes, the caching method can also be called directly. This can be useful when a source file is used in multiple
transformations, but always requires the same preprocessing before it can be used:

.. code-block:: python

    source = wrapper.source_data.get_source_file('source_table.csv')
    df = source.get_csv_as_df(apply_dtypes=True, cache=False)
    # Do some preprocessing on df here
    source.cache_df(df)

The next time :meth:`~.SourceFile.get_csv_as_df()` is called for this source, the cached dataframe will be returned,
meaning the preprocessing is already performed.

Whenever a data retrieval method is called without ``cache=True``, any cached objects of that respective data structure
that currently exist, will be dropped.

SAS files
---------
SAS files can be used by loading them as a pandas dataframe.
Column data types are typically already stored in the file format, but dtypes can be provided to overrule these.
The same caching options apply as for delimited text files.

.. code-block:: python

    source = wrapper.source_data.get_source_file('source_table.sas7bdat')
    df = source.get_sas_as_df(apply_dtypes=False)
    # Do some preprocessing on df here
    source.cache_df(df)

Read from database table
------------------------
If using :ref:`raw sql queries <transformations:Raw SQL>`
or the :ref:`SQL expression language <transformations:SQLAlchemy query>`
for transformations, the source data will first need to be inserted into the database.
This one-off effort will need to be done outside of delphyne, after which the database tables can be used in
transformations.
