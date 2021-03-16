Source to concept map
=====================

.. contents::
    :local:
    :backlinks: none

The SOURCE_TO_CONCEPT_MAP (STCM) table is a legacy vocabulary table that allows to store mappings from
source codes to standard OMOP concept_ids. Aside from documenting semantic mapping choices inside your database,
it can be conveniently used for lookups during the ETL process - see :ref:`STCM mappings` for details.

STCM files
----------

STCM records should be be provided in one or multiple comma-separated (csv) files with an (optional) ``_stcm`` suffix.
The typical way to generate a STCM file is to export existing mappings from `Usagi <https://github.com/OHDSI/Usagi>`_
in the STCM format (*File > Export source_to_concept_map*).

STCM files may contain source codes from one or more custom vocabularies.
When a STCM file contains records from a single custom vocabulary,
it's good practice to prepend the vocabulary_id to the file name (e.g. MYVOCAB_stcm.csv).
This way, if the mapping version associated with that vocabulary hasn't changed (see `Versioning`_),
the file will be ignored without needing to parse the file contents.

Each STCM file is expected to contain a header with field names matching the column names
of the SOURCE_TO_CONCEPT_MAP table (in lowercase).
Below is an **STCM file example**; in addition to the fields shown, it must contain
``valid_start_date``, ``valid_end_date`` (value is mandatory),
and ``source_code_description``, ``invalid_reason`` (value is optional):

.. list-table:: mixed_vocabs_stcm.csv
   :widths: auto
   :align: left
   :header-rows: 1

   * - source_code
     - source_concept_id
     - source_vocabulary_id
     - target_concept_id
     - target_vocabulary_id
   * - ABC
     - 0
     - MY_VOCAB1
     - 40305063
     - SNOMED
   * - XYZ
     - 2000000001
     - MY_VOCAB2
     - 439676
     - SNOMED

The ``source_concept_id`` field should be set to ``0`` or a custom concept_id above 2 billion;
in the latter case, make sure to load the corresponding CONCEPT records to the vocabulary tables
(see instructions in :ref:`Custom vocabularies`).

In addition to STCM files, you must always provide a single tab-separated file named **stcm_versions.tsv**,
containing the header fields shown below (in lowercase). This file maps to the SOURCE_TO_CONCEPT_MAP_VERSION table
and is needed for the versioning of STCM records (see `Versioning`_).

.. list-table:: stcm_versions.tsv
   :widths: auto
   :align: left
   :header-rows: 1

   * - source_vocabulary_id
     - stcm_version
   * - MY_VOCAB1
     - 0.1
   * - MY_VOCAB2
     - 0.1

Files have to be placed in te following folder:

::

    project_root
    └── resources
        └── vocabularies
            └── source_to_concept_map
                ├── MYVOCAB_stcm.csv
                ├── mixed_vocabs_stcm.csv
                ├── stcm_versions.tsv
                └── etc.

.. note::
   STCM files are expected in the csv format (as opposed to stcm_versions.tsv and other CDM tables)
   because this is currently the STCM export format provided by Usagi.

Make sure to add any custom vocabulary used in the stcm tables to the custom vocabulary folder
(see :ref:`Custom Vocabulary files`).

Add to pipeline
---------------

To load STCM files, set the ``skip_source_to_concept_map_loading`` option in your config.yml to ``False``,
and add the following call to your Wrapper's run method:

.. code-block:: python

   self.vocab_manager.stcm.load()

If you are loading **custom vocabularies associated with the STCM records**
(typically the case when loading STCM tables for the first time),
also set ``skip_custom_vocabulary_loading`` to ``False`` in config.yml,
and replace the previous call with the following in the Wrapper's run method:

.. code-block:: python

   self.vocab_manager.load_custom_vocab_and_stcm_tables()

Versioning
----------

STCM records are always associated with a mapping version (``stcm_version``) through their ``source_vocabulary_id``;
this relationship is captured in the SOURCE_TO_CONCEPT_MAP_VERSION table.

When you update the mapping version for a given ``source_vocabulary_id`` in the ``stcm_versions.tsv`` file,
this will update the SOURCE_TO_CONCEPT_MAP_VERSION table, and cause all SOURCE_TO_CONCEPT_MAP records associated with
that vocabulary to be dropped and replaced with new records from the provided STCM files, if any.
If you remove a ``source_vocabulary_id`` from ``stcm_versions.tsv``, associated records will also be dropped.

.. note::
   The SOURCE_TO_CONCEPT_MAP_VERSION table is not part of the standard OMOP CDM. We specifically introduced it in our
   ORM model to enable versioning of STCM records; this in turn makes easier to automate operations such as
   updating and deleting records.

STCM tables cleanup
-------------------

To wipe clean both the SOURCE_TO_CONCEPT_MAP and SOURCE_TO_CONCEPT_MAP_VERSION tables,
use the :meth:`~.StcmLoader.delete()` call in the Wrapper's run method:

.. code-block:: python

   self.vocab_manager.stcm.delete()

You can optionally pass a set of ``source_vocabulary_id`` to the ``vocab_ids`` parameter to selectively remove
STCM records associated with those vocabulary ids.
