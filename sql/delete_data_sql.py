def delete_mart_data(project_id, dataset, table_nm, execute_date, **context):
    sql = """
        DELETE FROM `{0}.{1}.{2}`
        WHERE event_date = DATE('{3}')
    """.format(project_id, dataset, table_nm, execute_date)
    
    return BigQueryOperator(
        task_id='delete_mart_data',
        sql=generate_delete_query,
        use_legacy_sql=False,
        bigquery_conn_id='bigquery_conn',
        dag=dag
    )
