Code mapper
===========

.. contents::
    :local:
    :backlinks: none

Mapping source codes to standard concept_ids
--------------------------------------------

`delphyne`'s :class:`.CodeMapper` class enables the creation
of mapping dictionaries from non-standard vocabulary term to valid standard `concept_id`.
Mappings are built on information extracted from the CONCEPT and CONCEPT_RELATIONSHIP tables.
For example, it is possible to automatically map ICD10CM ontology terms to their SNOMED standard `concept_id` equivalent.
Once created, a mapping dictionary can be used in any transformation to quickly lookup target `concept_id` information
for any source term.

Note that creating a mapping dictionary is only possible for official (non-standard) OMOP vocabularies;
if a source vocabulary is not available from `Athena <https://athena.ohdsi.org/vocabulary/list>`_,
other mapping methods should be considered (e.g. loading the mappings directly from file).

Creating a mapping dictionary
-----------------------------

Mapping dictionaries (in fact, :class:`.MappingDict` objects) can be created
for one or multiple source vocabularies at once, identified by their OMOP `vocabulary_id`.
When possible, it is recommended to use the `restrict_to_codes` argument to load mappings
only for the source codes that will be actually used in the transformations, thus limiting memory usage.

`delphyne`'s Wrapper provides a ready-to-use :class:`.CodeMapper` instance under
the attribute `code_mapper`. You can use it to create a mapping dictionary inside a transformation script as follows:

.. code-block:: python

   mapping_dict = wrapper.code_mapper.generate_code_mapping_dictionary(
               vocabulary_id='ICD10CM',
               restrict_to_codes=['R51', 'T68', 'B36.0'])

If the same mapping dictionary is needed in several transformations, it might be more efficient to instantiate
it as a wrapper attribute and reuse it, since the dictionary creation can take up considerable time.
This is especially true for long `restrict_to_codes` lists, or when no `restrict_to_codes` argument is provided.

Using a mapping dictionary
----------------------------

The :meth:`.MappingDict.lookup()` method allows to retrieve mapping information for a single source term at a time:

.. code-block:: python

   mapping = mapping_dict.lookup(source_code='R51')

The method retrieves by default a list of :class:`.CodeMapping` objects,
capturing information about both the source and target terms.
The previous lookup for example will return a list composed of a single :class:`.CodeMapping` object,
with the following attributes:

.. code-block:: python

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
a single :class:`.CodeMapping` object with both `source_concept_id` and `target_concept_id` set to `0`.

Use the option `target_concept_id_only=True` to retrieve any matching `target_concept_id` (as integer) instead
of full mapping objects. Use `first_only=True` to retrieve the first available match instead of a list of matches.
