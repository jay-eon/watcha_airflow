def delete_mart_data(project_id, dataset, table_nm, execute_date, **context):
    delete_sql = """
        DELETE FROM `{0}.{1}.{2}`
        WHERE event_date = DATE('{3}')
    """.format(project_id, dataset, table_nm, execute_date)
    
    return delete_sql
