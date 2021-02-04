# delphyne
<p align="center">
  <a href="https://github.com/thehyve/delphyne/actions">
  <img src="https://github.com/thehyve/delphyne/workflows/build/badge.svg" /></a>

  <a href='https://delphyne.readthedocs.io/en/latest/?badge=latest'>
  <img src='https://readthedocs.org/projects/delphyne/badge/?version=latest' alt='Documentation Status' /></a>

  <a href="https://pypi.python.org/pypi/delphyne">
  <img src="https://img.shields.io/pypi/v/delphyne.svg" /></a>
  
  <a href="https://pypi.org/project/delphyne">
  <img src="https://img.shields.io/pypi/pyversions/delphyne" /></a>

  <a href="https://codecov.io/gh/thehyve/delphyne">
  <img src="https://codecov.io/gh/thehyve/delphyne/branch/master/graph/badge.svg?token=48Z1TCIU8R"/></a>
</p>

delphyne is a Python package designed to simplify and standardize the process of converting
source data into the [OMOP Common Data Model](https://www.ohdsi.org/data-standardization/the-common-data-model/).

Full documentation is available on [Read the Docs](https://delphyne.readthedocs.io/en/latest/).

## Main Features
-   Customizable SQLAlchemy ORM of multiple OMOP CDM releases and extensions
-   Flexible data transformation options using either raw SQL or the ORM
-   Built-in tracking of ETL transformations to generate an overview of table record changes
-   Comprehensive integration of source data files into transformations, with options for caching and various data formats
-   Intuitive table constraint management ranging from a single constraint/index to the full CDM
-   Integrated loading functionality for standard vocabularies, custom vocabularies and source to concept mappings

## Installation
The package is available on PyPI. To install the latest version:

```sh
pip install delphyne
```

## Template
It is strongly recommended using this package in combination with [delphyne-template](https://github.com/thehyve/delphyne-template),
as it already contains all the expected files/folders, and a basic ETL framework with example transformations.

## Tests
Make sure Docker is running if you want to run the full test suite.
Otherwise, tests that require it will be skipped.

```sh
# run tests in your current Python environment
pytest

# run tests for all supported Python versions
nox -s tests
```

## License
[GPLv3](https://github.com/thehyve/delphyne/blob/master/LICENSE)

## Credits
This package was created with Cookiecutter and the `cs01/cookiecutter-pypackage` project template.

[Cookiecutter](https://github.com/audreyr/cookiecutter)

[cs01/cookiecutter-pypackage](https://github.com/cs01/cookiecutter-pypackage)
