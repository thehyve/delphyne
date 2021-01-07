# delphyne
<p align="center">
  <a href="https://github.com/thehyve/delphyne/actions">
  <img src="https://github.com/thehyve/delphyne/workflows/build/badge.svg" /></a>
  <a href="https://pypi.python.org/pypi/delphyne">
  <img src="https://img.shields.io/pypi/v/delphyne.svg" /></a>
  <a href="https://codecov.io/gh/thehyve/delphyne">
  <img src="https://codecov.io/gh/thehyve/delphyne/branch/master/graph/badge.svg?token=48Z1TCIU8R"/></a>
</p>
Wrapper for OMOP ETL projects

## Features
-   TODO


## Installation
The package is currently not (yet) available on PyPI, therefore a local installation is needed.

```sh
git clone https://github.com/thehyve/delphyne.git
cd delphyne

# regular installation
pip install .

# or install in editable mode (including test dependencies)
pip install -e '.[TEST]'
```

## Tests
Make sure Docker is running if you want to run the full test suite.
Otherwise, tests that require it will be skipped.

```sh
# run tests in your current Python environment
pytest

# run tests for all supported Python versions
nox -s tests
```

## Credits
This package was created with Cookiecutter and the `cs01/cookiecutter-pypackage` project template.

[Cookiecutter](https://github.com/audreyr/cookiecutter)

[cs01/cookiecutter-pypackage](https://github.com/cs01/cookiecutter-pypackage)
