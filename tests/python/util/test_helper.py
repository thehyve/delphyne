from src.omop_etl_wrapper.util.helper import get_full_table_name


def test_get_full_table_name_no_schema():
    name = get_full_table_name(table='table1', schema=None)
    assert name == 'table1'


def test_get_full_table_name_no_schema_map():
    name = get_full_table_name(table='table1', schema='schema1')
    assert name == 'schema1.table1'


def test_get_full_table_name_with_schema_map():
    name = get_full_table_name(table='table1', schema='schema1',
                               schema_map={'schema1': 'schema2'})
    assert name == 'schema2.table1'
