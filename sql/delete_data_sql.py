def generate_delete_query(project_id, dataset, table_nm, execute_date,**context):
    sql = """
        DELETE FROM `{0}.{1}.{2}`
        WHERE event_date = DATE('{3}')
    """.format(project_id, dataset, table_nm, execute_date)
    
    return sql
