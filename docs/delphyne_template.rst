Building a custom ETL with delphyne-template
============================================

.. contents::
    :local:
    :backlinks: none


delphyne is meant to be used as a dependency in your custom ETL implementation:
you would typically install it in a virtual environment, and import specific modules and classes
as needed to build your pipeline. At the very least, your delphyne-based code would need to specify the target CDM model,
and define a subclass of delphyne's ``Wrapper`` that implements the ``run()`` method for executing transformations.
You should also make sure to satisfy delphyne's assumptions regarding folders and scripts locations,
as described in detail in this documentation.

To simplify the setup process, we **strongly recommend** building your ETL implementation on **delphyne-template**,
a ready-to-use ETL backbone for converting data to the OMOP CDM.
delphyne-template is kept in sync with the latest version of delphyne, and ensures the optimal use of the package
by providing a standard folder structure, a customizable wrapper implementation, an editable CDM model,
and configuration files for database connections and source data loading, among other features.

delphyne-template is available on `GitHub <https://github.com/thehyve/delphyne-template>`_.
Simply click on the "Use this template" button in the main page to create a new repository for your project.

Folder structure overview
-------------------------

A project built on delphyne-template will be initially structured as follows:

::

    project_root/
    ├── config/
    ├── docs/
    ├── resources/
    │   ├── mapping_tables/
    │   ├── synthetic_data/
    │   └── vocabularies/
    ├── src/
    │   ├── main/
    │   │   ├── python/
    │   │   │   ├── cdm/
    │   │   │   ├── transformation/
    │   │   │   ├── util/
    │   │   │   └── wrapper.py
    │   │   └── sql/
    │   └── test/
    └── main.py

Mandatory folders
^^^^^^^^^^^^^^^^^

- config
- resources/vocabularies
- src

Optional folders
^^^^^^^^^^^^^^^^

These folders can be safely removed if not used:

- docs
- resources/mapping_tables
- resources/synthetic_data

Getting started
---------------

Ensure you can execute these steps successfully to connect to the target database, drop and create required schemas,
and populate them with the standard vocabularies.

- install dependencies
- configure connection to one of the supported DBMS (currently PostgreSQL)
- configure data sources (typically synthetic data)
- configure logging level
- customize target CDM model
- load standard vocabularies
- don't forget to update the project README (README-sample)!

ETL customization
-----------------

Implement a custom ETL.

- write transformation scripts (python / sql)
- customize wrapper run() method
- load custom vocabularies
- load custom mappings
- write tests
- change logging options + write reports
