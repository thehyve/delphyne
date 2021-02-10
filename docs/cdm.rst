Defining the CDM
================

.. contents::
    :local:
    :backlinks: none

delphyne uses the SQLAlchemy declarative ORM for the table mappings.
For more general information on how to use the declarative system, check out the
`SQLAlchemy documentation <https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/index.html>`_.

CDM version
-----------
Before you can start writing transformations, the target CDM model has to be defined.
delphyne provides SQLAlchemy table definitions for multiple CDM releases, and also includes CDM extensions.
In delphyne-template you can find an example on how to select which tables to include
at ``src/main/python/cdm/cdm/tables.py``. By default, CDM release 6.0.0 is selected.
If you want to map to CDM 5.3.1 instead, remove all CDM 6.0.0 specific tables and uncomment the CDM 5.3.1 table lines.
E.g. for the ``person`` table, remove lines

.. code-block:: python

    class Person(BasePersonCdm600, Base):
        pass

and replace with

.. code-block:: python

    class Person(BasePersonCdm531, Base):
        pass

to use the CDM 5.3.1 version of this table.

Some (vocabulary) tables are not version-specific and can be left as they are.

.. note::
   Although not impossible to use a mix of tables from multiple CDM versions, it's recommended
   to stick to tables from the same release.

CDM extensions
--------------
In addition to the standard CDM tables, you can also add tables from CDM extensions.
These can be imported from ``delphyne.cdm.cdm_extensions``. Near the bottom of ``cdm/tables.py`` in
delphyne-template, you'll find an example of how to do this for the
`oncology extension <https://ohdsi.github.io/CommonDataModel/oncology.html>`_.

Make sure that if an extension contains altered versions of existing CDM tables,
you only define the class for the version from the extension and not the default version.
E.g. if the measurement table from the oncology extension is used,
you cannot also use the regular measurement table, which should therefore be removed.

Legacy tables
-------------
At the bottom of ``cdm/tables.py`` in delphyne-template is a segment for CDM legacy tables.
These tables have been part of the standard CDM in previous releases, but have since been moved
to the results schema or removed entirely. These are the cohort, cohort_definition, cohort_attribute
and attribute_definition tables. They are not included by default, but if you do want to use them,
you can uncomment the lines to make them part of the ``cdm_schema``.

Modify CDM tables
-----------------

Add new columns
^^^^^^^^^^^^^^^
If desired, you can add custom fields to existing CDM tables.
To do this, define the table class as normal, but add additional ``Column`` fields with the
data types of choice. E.g. to add an additional integer column to the person table:

.. code-block:: python

    class Person(BasePersonCdm600, Base):
        favorite_number = Column(Integer, nullable=True)

Replace columns
^^^^^^^^^^^^^^^
Although not recommended, you can replace existing CDM table columns with a different type of column.
E.g. removing the character limit of the ``ethnicity_source_value`` column in the person table, by replacing
it with a ``Text`` data type, can be done as follows:

.. code-block:: python

    class Person(BasePersonCdm600, Base):
        ethnicity_source_value = Column(Text, nullable=True)

It's important to note that replacing columns can only be done if the replacement
doesn't break relationships with other tables.
Replacing the ``BigInt`` ``person_id`` column in the person table with a column of type ``Text`` for example,
will not work as it breaks FK relationships that other CDM tables have with this field.

Replace whole table
^^^^^^^^^^^^^^^^^^^
Instead of adding or replacing individual columns, it's also possible to replace an entire table.
To do that, remove the inherited table base class from your table class and define all fields yourself.
E.g. for the person table:

.. code-block:: python

    class Person(Base):
        __tablename__ = 'person'
        __table_args__ = {'schema': 'cdm_schema'}
        # Define all columns here

Just like with modifying individual columns, this will only work if no violations occur in relationships
with other CDM tables.

Add new tables
--------------
In addition to the default CDM tables, you can also add your own custom tables to the model.
You can do so by either directly adding the table definitions in the same module, or (preferably) by
putting them in ``custom/tables.py``.

By adding these tables to the same declarative ``Base`` as the regular tables, they will become part
of the ORM. For example:

.. code-block:: python

    # cdm/custom/tables.py

    from ..cdm.tables import Base


    class Narcissus(Base):
        __tablename__ = 'narcissus'
        __table_args__ = {'schema': 'cdm_schema'}

        person_id = Column(ForeignKey('cdm_schema.person.person_id'), nullable=False)
        loved_by_narcissus = Column(Boolean, default=False)

        person = relationship('Person')

Schema name
^^^^^^^^^^^
When adding your own tables, it's a good practice to specify a schema name via ``__table_args__`` (see example above).
If the schema will always be the same, there is no harm in hard coding the name.
Otherwise, it's better to provide a schema placeholder name, and let the runtime schema name be determined by the
contents of your main config file.
