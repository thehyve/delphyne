Code mapper
===================

.. contents::
    :local:
    :backlinks: none

Mapping source codes to standard concept_ids
--------------------------------------------

`delphyne`'s CodeMapper class enables the creation of mapping dictionaries from non-standard vocabulary terms to
valid standard `concept_id`s. Mappings are built on information extracted from the CONCEPT and CONCEPT_RELATIONSHIP tables.
For example, it is possible to automatically map ICD10CM ontology terms to their SNOMED standard `concept_id` equivalent.
Once created, a mapping dictionary can be used in any transformation to quickly lookup target `concept_id` information for any source term.

Note that creating a mapping dictionary is only possible for official (non-standard) OMOP vocabularies;
if a source vocabulary is not available from `Athena <https://athena.ohdsi.org/vocabulary/list>`_,
other mapping methods should be considered (e.g. loading the mappings directly from file).

Creating a mapping dictionary
-----------------------------

Mapping dictionaries (in fact, `MappingDict` objects) can be created for one or multiple source vocabularies at once,
identified by their OMOP `vocabulary_id`. When possible, it is recommended to use the `restrict_to_codes` argument
to load mappings only for the source codes that will be actually used in the transformations, thus limiting memory usage.

.. code-block:: python

   mapping_dict = self.code_mapper.generate_code_mapping_dictionary(
            'ICD10CM', restrict_to_codes=['R51', 'T68', 'B36.0'])

If the same mapping dictionary is going to be used in several transformations, it is more efficient to instantiate it
as a wrapper attribute since repeated dictionary creation can take up considerable time.

Using a mapping dictionary
----------------------------

The dictionary `lookup()` method provides a way to retrieve mapping information for a single source term at a time:

.. code-block:: python

   mapping = mapping_dict.lookup('R51')

The method retrieves by default a list of `CodeMapping` objects, capturing information about both the source and target terms.
If you are only interested in the target `concept_id`, use the option `target_concept_id_only=True`.
Also note that sometimes a single source term can have multiple mappings to standard `concept_id`;
to return a single match in all cases, use `first_only=True`.

