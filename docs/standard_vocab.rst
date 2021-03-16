Standard vocabularies
=====================

.. contents::
    :local:
    :backlinks: none


Vocabulary files
----------------

All standardized data in the OMOP CDM relies on the contents of the vocabulary tables.
Loading the standard vocabularies is therefore typically one of the first steps in an ETL pipeline.
To get the vocabulary files, go to the download section of `Athena <https://athena.ohdsi.org/vocabulary/list>`_,
select the vocabularies you would like to include and click **Download Vocabularies**.

The zipped download contains the vocabulary contents as csv files, which should then be added to
the standard vocabularies folder:

::

    project_root
    └── resources
        └── vocabularies
            └── standard
                ├── CONCEPT.csv
                ├── CONCEPT_ANCESTOR.csv
                ├── CONCEPT_CLASS.csv
                ├── CONCEPT_RELATIONSHIP.csv
                ├── CONCEPT_SYNONYM.csv
                ├── DOMAIN.csv
                ├── DRUG_STRENGTH.csv
                ├── RELATIONSHIP.csv
                └── VOCABULARY.csv

Add to pipeline
---------------

Make sure the ``load_vocabulary`` option in your config.yml is set to ``True``.
As part of your Wrapper's run method, the following line must be included:

.. code-block:: python

   self.vocab_manager.standard_vocabularies.load()

This will drop all indexes and constraints on the tables before insertion, and restores them afterwards.

To prevent accidental reloading of vocabularies, they can only be loaded if all the target tables are empty.
If you do want to reload them, you therefore need to drop the contents manually.


Limitations
-----------

As SQL Server's bulk insert method requires a local file path, it can only be used if you're running
delphyne in the same environment as the database.
To insert the vocabularies manually, you can find instructions in
the `CommonDataModel repository <https://github.com/OHDSI/CommonDataModel>`_.
