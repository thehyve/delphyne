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


**delphyne** is a Python package designed to simplify and standardize the process of converting
source data to the `OMOP Common Data Model (CDM) <https://www.ohdsi.org/data-standardization/the-common-data-model/>`_.

delphyne offers several benefits to ETL builders:

- **easy setup**:
  delphyne automates tedious manual operations, such as the creation of target database schemas and tables,
  and the loading of standard OMOP vocabularies;
- **flexibility**:
  you can read source data from file or a supported relational database,
  set the target CDM model to an official version or a custom one,
  and implement transformations in Python or SQL as you feel comfortable;
- **performance**:
  delphyne provides several options for efficient data extraction and loading,
  by making use of caching and other performance optimization techniques.

Additionally, we provide a ready-to-use ETL framework, **delphyne-template**,
which used together with delphyne ensures a fast and optimal project setup.

Features overview
-----------------

-   Customizable SQLAlchemy ORM for multiple target OMOP CDM releases and extensions
-   Integrated loading functionality for standard vocabularies, custom vocabularies, and source to concept mappings
-   Flexible data transformation options using either raw SQL, SQLAlchemy queries, or the SQLAlchemy ORM
-   Built-in tracking of ETL transformations to generate an overview of table record changes
-   Easy use of source data files in transformations, with options for caching and various data formats
-   Intuitive table constraint management ranging from a single constraint/index to the full CDM
-   Efficient tools to map source values to standard concept_ids, based on OMOP vocabularies or custom source to concept mappings

Source data can be extracted from file or a supported relational DBMS;
data read directly from a database will be converted and loaded to a new schema in the same database.

Supported CDM versions
^^^^^^^^^^^^^^^^^^^^^^

delphyne offers out-of-the-box ORM models for **CDM v5.3.1 and 6.0.0**,
the Oncology extension, and legacy CDM tables.
Additionally, you can create your own model.

Supported DBMSs
^^^^^^^^^^^^^^^

delphyne can be used with **PostgreSQL** and **Microsoft SQL Server** databases.

Requirements
------------

**Python 3.7.2+** (tested with 3.7-3.9).

Installation
------------

For ETL builders
^^^^^^^^^^^^^^^^

Please follow the instructions on how to build a conversion ETL to the OMOP CDM using delphyne-template
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
