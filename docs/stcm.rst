Source to concept map
=====================

.. contents::
    :local:
    :backlinks: none

The section describes available Delphyne's methods to map source data to standard OMOP concept_ids.

STCM files
----------

Non-standard vocabulary data can be provided as tab-delimited (tsv) files.
The file names must end with the name of the vocabulary table they should be inserted in (e.g. concept.tsv).
Currently, target tables concept, concept_class and vocabulary are supported.

The custom vocabulary files may contain data of one or more custom vocabularies.
When a vocabulary file contains data of only one custom vocabulary,
it's good practice to prepend the vocabulary_id to the file name (e.g. MYVOCAB_concept.tsv).
This way, if the vocabulary version hasn't changed (see `Versioning`_),
the file will be ignored without needing to parse the file contents.

For custom vocabularies, the vocabulary_concept_id (vocabulary) and
concept_class_concept_id (concept_class) should always be set to 0.

Files have to be placed in te following folder:

::

    project_root
    └── resources
        └── vocabularies
            └── source_to_concept_map
                ├── MYVOCAB_stcm.tsv
                ├── mixed_vocabs_stcm.tsv
                ├── stcm_versions.tsv
                └── etc.

Make sure to add any custom vocabulary used in the stcm tables to the custom vocabulary folder
(see :ref:`Custom Vocabulary files`).

Add to pipeline
---------------

Make sure the ``skip_source_to_concept_map_loading`` option in your config.yml file is set to ``False``.

To load STCM files during ETL execution, add the following call to your Wrapper's run method:

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
that vocabulary to be dropped and replaced with new records from the ``source_to_concept_map`` folder, if any.

STCM tables cleanup
-------------------

To wipe clean both the SOURCE_TO_CONCEPT_MAP and SOURCE_TO_CONCEPT_MAP_VERSION tables,
use the :meth:`~.StcmLoader.delete()` call in the Wrapper's run method:

.. code-block:: python

   self.vocab_manager.stcm.delete()

You can optionally pass a ``vocab_ids`` parameter containing a set of ``source_vocabulary_id`` to be removed selectively.
