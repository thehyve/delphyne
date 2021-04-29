delphyne
========

.. toctree::
   :maxdepth: 2
   :hidden:

   delphyne_template
   cdm
   transformations
   standard_vocab
   custom_vocab
   stcm
   semantic_mapping
   reference
   changelog_link


delphyne is a Python package designed to simplify and standardize the process of converting
source data into the `OMOP Common Data Model <https://www.ohdsi.org/data-standardization/the-common-data-model/>`_.

Main Features
-------------

-   Customizable SQLAlchemy ORM of multiple OMOP CDM releases and extensions
-   Flexible data transformation options using either raw SQL or the ORM
-   Built-in tracking of ETL transformations to generate an overview of table record changes
-   Easy use of source data files in transformations, with options for caching and various data formats
-   Intuitive table constraint management ranging from a single constraint/index to the full CDM
-   Integrated loading functionality for standard vocabularies, custom vocabularies and source to concept mappings

Requirements
------------

Python 3.7.2 or higher (tested with 3.7-3.9).

Installation
------------

For ETL builders (delphyne-template)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

ETL builders are strongly recommended to setup their project using `delphyne-template <https://github.com/thehyve/delphyne-template>`_,
as it provides a ready-to-use ETL framework, complete with all files and folders required by delphyne.

We provide detailed instructions on how to build your ETL project with delphyne-template
in the section :ref:`delphyne_template:Getting started with delphyne`.

For developers (stand-alone)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are installing delphyne for other development purposes,
you can get the latest version from PyPI:

.. code-block:: bash

   pip install delphyne

Alternatively, you can install from source:

.. code-block:: bash

   git clone https://github.com/thehyve/delphyne.git
   cd delphyne

   # regular installation
   pip install .

   # or install in editable mode (including test dependencies)
   pip install -e '.[TEST]'

Additional instructions for developers, such as for testing and generating documentation, are available in the
`CONTRIBUTING.md <https://github.com/thehyve/delphyne/blob/master/CONTRIBUTING.md>`_ file in delphyne's repository.

License
-------

`GPLv3 <https://github.com/thehyve/delphyne/blob/master/LICENSE>`_

Credits
-------

This package was created with `Cookiecutter <https://github.com/audreyr/cookiecutter>`_
and the `cs01/cookiecutter-pypackage <https://github.com/cs01/cookiecutter-pypackage>`_ project template.
