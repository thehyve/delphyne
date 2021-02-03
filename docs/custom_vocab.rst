Custom vocabularies
===================

.. contents::
    :local:
    :backlinks: none

Custom Vocabulary files
-----------------------

Non-standard vocabulary data
Loading the standard vocabularies is therefore typically one of the first steps in an ETL pipeline.


The zipped download contains the vocabulary contents as csv files, which should then be added to
the standard vocabularies folder:

::

    project_root
    └── resources
        └── vocabularies
            └── custom
                ├── MYVOCAB_vocabulary.tsv
                ├── MYVOCAB_concept.tsv
                ├── vocabulary.tsv
                └── etc.

Add to pipeline
---------------

Make sure the `skip_custom_vocabulary_loading` option in your config.yml is set to False.
As part of your Wrapper's run method, the following line must be included.

.. code-block:: python

   self.vocab_manager.standard_vocabularies.load()

This will drop all indexes and constraints on the tables before insertion, and restores them afterwards.

To prevent accidental reloading of vocabularies, they can only be loaded if all the target tables are empty.
If you do want to reload them, you therefore need to drop the contents manually.

