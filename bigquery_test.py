import airflow
from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_dag_args = {
      'owner': 'airflow',
      'start_date': airflow.utils.dates.days_ago(2),
      'email': ['wodus2621@gmail.com'],
      'email_on_failure': True,
      'email_on_retry': False,
      'retries': 0,
      'project_id': 'seventh-coast-386809'
}

query = """
    SELECT *
    FROM `seventh-coast-386809.watcha.events_info`
    LIMIT 1000
"""
	
with models.DAG(
    dag_id = 'bigquery_test',
    schedule_interval= '0 0 * * *',
    default_args=default_dag_args
    ) as dag:

    bq_query = BigQueryOperator(
        task_id='extract_events_schema',
        sql=query, 
        use_legacy_sql=False,
        destination_dataset_table='watcha.event_schema',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_conn'
    )
	
    bq_query