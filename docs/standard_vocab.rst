Loading standard vocabularies
=============================

.. contents::
    :local:
    :backlinks: none

.. note::
   delphyne currently only supports postgresql for loading standard vocabularies. For other dialects
   please use the instructions on the `CommonDataModel repository <https://github.com/OHDSI/CommonDataModel>`_.

All standardized data in the OMOP CDM relies on the contents of the vocabulary tables.
Loading the standard vocabularies is therefore typically one of the first steps in an ETL pipeline.
To get the vocabulary files, go to the download section of `Athena <https://athena.ohdsi.org/vocabulary/list>`_
, select the vocabularies you would like to include and click **Download Vocabularies**.

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
