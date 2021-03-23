Semantic mapping tools
======================

.. contents::
    :local:
    :backlinks: none

The section describes available delphyne's tools to map source data to standard OMOP concept_ids during ETL execution.

Vocabulary-based mappings
-------------------------

delphyne's :class:`.CodeMapper` class enables the creation of mapping dictionaries from non-standard OMOP vocabulary
terms to valid standard concept_ids. Once created, a mapping dictionary can be used in any transformation to quickly
lookup mappings for any source term.

Mappings are built on information extracted from the CONCEPT and CONCEPT_RELATIONSHIP tables. For example, it is
possible to automatically extract mappings from ICD10CM ontology terms to their equivalent standard concept_id
(typically SNOMED).

Note that creating a mapping dictionary with this method is only possible for **official OMOP vocabularies**;
if a source vocabulary is not available from `Athena <https://athena.ohdsi.org/vocabulary/list>`_,
alternative methods should be considered.

Creating a mapping dictionary
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Mapping dictionaries (in fact, :class:`.MappingDict` objects) can be created
for one or multiple source vocabularies at once, identified by their OMOP vocabulary_id.
When possible, it is recommended to use the ``restrict_to_codes`` argument to load mappings
only for the source codes that will be actually used in the transformations, thus limiting memory usage.

Delphyne's :class:`.Wrapper` class provides a ready-to-use :class:`.CodeMapper` instance under the attribute
``self.code_mapper``. You can use it to create a mapping dictionary inside a transformation script as follows:

.. code-block:: python

    mapping_dict = wrapper.code_mapper.generate_code_mapping_dictionary(
        vocabulary_id='ICD10CM',
        restrict_to_codes=['R51', 'T68', 'B36.0']
    )

.. note::
   If the same mapping dictionary is needed in several transformations, it might be more efficient to instantiate
   it as a Wrapper attribute and reuse it, since the dictionary creation can take up considerable time.
   This is especially true for long ``restrict_to_codes`` lists, or when no ``restrict_to_codes`` argument is provided.

Using a mapping dictionary
^^^^^^^^^^^^^^^^^^^^^^^^^^

The :meth:`.MappingDict.lookup()` method allows to retrieve mapping information for a single source term at a time:

.. code-block:: python

   mappings = mapping_dict.lookup(source_code='R51')

The method retrieves by default a list of :class:`.CodeMapping` objects,
capturing information about both the source and target terms.
The previous lookup for example will return a list composed of a single :class:`.CodeMapping` object,
with the following attributes:

.. code-block:: python

   mapping = mappings[0]

   mapping.source_concept_code      # 'R51'
   mapping.source_concept_id        # 35211388
   mapping.source_concept_name      # 'Headache'
   mapping.source_vocabulary_id     # 'ICD10CM'
   mapping.source_standard_concept  # None
   mapping.source_invalid_reason    # None
   mapping.target_concept_code      # '25064002'
   mapping.target_concept_id        # 378253
   mapping.target_concept_name      # 'Headache'
   mapping.target_vocabulary_id     # 'SNOMED'

If a code is not found in the mapping dictionary, :meth:`~.MappingDict.lookup()` returns a list containing
a single :class:`.CodeMapping` object with both ``source_concept_id`` and ``target_concept_id`` set to ``0``.

Use the option ``target_concept_id_only=True`` to retrieve a list of ``target_concept_id`` instead of full mapping objects.
Use ``first_only=True`` to retrieve the first available match instead of a list of all matches.

STCM mappings
-------------

The :class:`.Wrapper` class provides a :meth:`~.Wrapper.lookup_stcm()` method to extract mappings from the
SOURCE_TO_CONCEPT_MAP table. Note that you will need to populate the table yourself before being able to use this
method (see :ref:`stcm:Source to concept map` for instructions).

Looking up STCM mappings
^^^^^^^^^^^^^^^^^^^^^^^^

You can lookup mapping information for a single source code at a time as follows:

.. code-block:: python

   mapping = wrapper.lookup_stcm(source_vocabulary_id='MY_VOCAB', source_code='ABC')

The result is a single standard OMOP concept_id, or ``0`` if nothing is found.
