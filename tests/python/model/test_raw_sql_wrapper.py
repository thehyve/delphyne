from src.omop_etl_wrapper.model.raw_sql_wrapper import RawSqlWrapper


def test_apply_sql_parameters():
    prepared_statement = "SELECT @col1 FROM @table1 WHERE @col2 = '@val1';"
    sql_parameters = {'col1': 'menu', 'table1': 'restaurant', 'col2': 'location', 'val1': 'End of the universe'}
    final_query = RawSqlWrapper.apply_sql_parameters(prepared_statement, sql_parameters)
    assert final_query == "SELECT menu FROM restaurant WHERE location = 'End of the universe';"
