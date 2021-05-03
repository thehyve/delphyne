delphyne
========

.. toctree::
   :maxdepth: 2
   :hidden:

   delphyne_template
   cdm
   transformations
   standard_vocab
   custom_vocab
   stcm
   semantic_mapping
   reference
   changelog_link


delphyne is a Python package designed to simplify and standardize the process of converting
source data to the `OMOP Common Data Model (CDM) <https://www.ohdsi.org/data-standardization/the-common-data-model/>`_.

delphyne is time-saving:
you can automate tedious manual operations, such as the loading of standard vocabularies,
and use one of the several built-in tools to quickly map source codes to standard concept_ids.

delphyne is flexible:
you can read source data from file or an existing relational database,
implement transformations in Python or SQL as you feel comfortable,
and set the target CDM model to a default version or a custom one.

delphyne is powerful:
it provides several options to improve the efficiency of data extraction and loading,
by making use of caching and other performance optimization techniques.

Once converted, the data will be available in a relational DBMS,
where it can be queried with any of the supported `OHDSI analytics tools <https://www.ohdsi.org/analytic-tools/>`_.

Main Features
-------------

-   Customizable SQLAlchemy ORM for multiple target OMOP CDM releases and extensions, including legacy tables
-   Integrated loading functionality for standard vocabularies, custom vocabularies, and source to concept mappings
-   Flexible data transformation options using either raw SQL, SQLAlchemy queries, or the SQLAlchemy ORM
-   Built-in tracking of ETL transformations to generate an overview of table record changes
-   Easy use of source data files in transformations, with options for caching and various data formats
-   Intuitive table constraint management ranging from a single constraint/index to the full CDM
-   Efficient mapping tools from source codes to standard OMOP concept_ids

Supported CDM models
--------------------

delphyne offers out-of-the-box ORM models for **CDM v5.3.1 and 6.0.0**,
and several extension and legacy tables definitions;
additionally, you can define your own custom model.

Supported DBMSs
---------------

delphyne currently supports **PostgreSQL** and **Microsoft SQL Server**.
For conversions from other source DBMSs, you will first need to dump the data to file.
When the source data is extracted directly from a database,
the converted data will be loaded to a different schema in the same database.

Requirements
------------

**Python 3.7.2+** (tested with 3.7-3.9).

Installation
------------

For OMOP CDM conversions
^^^^^^^^^^^^^^^^^^^^^^^^

Please follow the instructions on how to build a conversion ETL to the OMOP CDM using **delphyne-template**
in the section :ref:`delphyne_template:Getting started with delphyne`.

The template provides a ready-to-use ETL framework complete with all files and folders required by delphyne,
and easy to customize configurations and scripts, saving you loads of setup time.

For developers and contributors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are installing delphyne for other purposes,
you can get the latest version from PyPI:

.. code-block:: bash

   pip install delphyne

or install directly from source:

.. code-block:: bash

   git clone https://github.com/thehyve/delphyne.git
   cd delphyne

   # regular installation
   pip install .

   # or install in editable mode (including test dependencies)
   pip install -e '.[TEST]'

Additional instructions for contributors are available in the
`CONTRIBUTING.md <https://github.com/thehyve/delphyne/blob/master/CONTRIBUTING.md>`_ file.

License
-------

`GPLv3 <https://github.com/thehyve/delphyne/blob/master/LICENSE>`_

Credits
-------

This package was created with `Cookiecutter <https://github.com/audreyr/cookiecutter>`_
and the `cs01/cookiecutter-pypackage <https://github.com/cs01/cookiecutter-pypackage>`_ project template.
