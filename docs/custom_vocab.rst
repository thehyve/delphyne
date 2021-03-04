Custom vocabularies
===================

.. contents::
    :local:
    :backlinks: none

Custom Vocabulary files
-----------------------

Non-standard vocabulary data can be provided as tab-delimited (tsv) files.
The file names must end with the name of the vocabulary table they should be inserted in
(e.g. concept.tsv). File name matching is case-insensitive.
Currently, target tables CONCEPT, CONCEPT_CLASS, and VOCABULARY are supported.

The custom vocabulary files may contain data from one or more custom vocabularies.
When a vocabulary file contains data from only one custom vocabulary,
it's good practice to prepend the vocabulary_id to the file name (e.g. MYVOCAB_concept.tsv).
This way, if the vocabulary version hasn't changed (see `Versioning`_),
the file will be ignored without needing to parse the file contents.

For custom vocabularies, the ``vocabulary_concept_id`` (VOCABULARY) and
``concept_class_concept_id`` (CONCEPT_CLASS) should always be set to ``0``.
Additionally, CONCEPT records should always have a ``concept_id`` **above 2 billion**
(e.g. starting at ``2000000001``) to prevent conflicts with concept_ids from the official OMOP vocabularies.

Files have to be placed in te following folder:

::

    project_root
    └── resources
        └── vocabularies
            └── custom
                ├── MYVOCAB_concept.tsv
                ├── MYVOCAB_concept_class.tsv
                ├── MYVOCAB_vocabulary.tsv
                ├── mixed_vocabulary.tsv
                └── etc.

Add to pipeline
---------------

Make sure the ``skip_custom_vocabulary_loading`` option in your config.yml is set to False.

If you're also using STCM files, the recommended way to load
the files is with the following call as part of your Wrapper's run method:

.. code-block:: python

   self.vocab_manager.load_custom_vocab_and_stcm_tables()

This will temporarily disable foreign keys on the STCM tables, allowing custom vocabularies to be
replaced if still referenced from there. Otherwise, they can be loaded independently:

.. code-block:: python

   self.vocab_manager.custom_vocabularies.load()

Versioning
----------
Like standard vocabularies, custom vocabularies need to have a version as provided in the
``vocabulary_version`` field of the VOCABULARY table. If the provided version value is different
from what is currently loaded in the database (if anything), the vocabulary will be replaced with
the new version.
