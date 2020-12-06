import pytest
from src.omop_etl_wrapper import Wrapper


@pytest.mark.usefixtures("test_db")
@pytest.fixture(scope='function')
def cdm531_wrapper_with_tables_created(wrapper_cdm531: Wrapper) -> Wrapper:
    wrapper_cdm531.create_schemas()
    wrapper_cdm531.create_cdm()
    return wrapper_cdm531


@pytest.mark.usefixtures("test_db")
@pytest.fixture(scope='function')
def cdm600_wrapper_with_tables_created(wrapper_cdm600: Wrapper) -> Wrapper:
    wrapper_cdm600.create_schemas()
    wrapper_cdm600.create_cdm()
    return wrapper_cdm600
