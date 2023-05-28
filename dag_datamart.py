from datetime import date, datetime, timedelta
from airflow import DAG

from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

from sql.delete_data_sql import delete_mart_data
from sql.insert_data_sql import insert_mart_data
import pendulum

# 타임존 설정
local_tz = pendulum.timezone('Asia/Seoul')

# 빅쿼리 실행할 변수(프로젝트, 데이터셋, 테이블명) 가져오기
project_id = Variable.get('project_id')
dataset = Variable.get('dataset')
tb_indicator_mart = Variable.get('tb_indicator_mart')
tb_event_info = Variable.get('tb_event_info')

# 기본 세팅
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo = local_tz), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_delete_query(project_id, dataset, table_nm, execute_date, **context):
    
    delete_sql = delete_mart_data(project_id, dataset, table_nm, execute_date)
    print(delete_sql)
    
    delete_bigquery_task = BigQueryOperator(
        task_id='delete_mart_data',
        sql=delete_sql,
        use_legacy_sql=False,
        bigquery_conn_id='bigquery_conn',
        dag=dag
    )
    
    delete_bigquery_task.execute(context)


def generate_insert_query(project_id, dataset, table_nm, execute_date, **context):

    insert_sql = insert_mart_data(project_id, dataset, table_nm, execute_date)
    print(insert_sql)
    
    insert_bigquery_task = BigQueryOperator(
        task_id='insert_mart_data',
        sql=insert_sql,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='bigquery_conn',
        trigger_rule = TriggerRule.ALL_SUCCESS,
        dag=dag
    )
    
    insert_bigquery_task.execute(context)


# DAG 시작 (오전 7시마다 수행)
with DAG(
        'daily_indicator_datamart', 
        default_args=default_args, 
        schedule_interval='0 7 * * *',
        catchup=True
    ) as dag:
    
    yesterday_ds = '{{ yesterday_ds }}'
    ds = '{{ ds }}'
    tomorrow_ds = '{{ tomorrow_ds }}'
    
    delete_data_task = PythonOperator(
        task_id='delete_from_datamart',
        python_callable=generate_delete_query,
        op_kwargs={'project_id': project_id, 'dataset': dataset, 'table_nm': tb_indicator_mart, 'execute_date':ds },
        provide_context=True,
        dag=dag
    )
    
    failure = DummyOperator(
        task_id='failure',
        trigger_rule = TriggerRule.ALL_FAILED,
        dag=dag
    )
    
    start = DummyOperator(
        task_id='start',
        dag=dag
    )
    
    end = DummyOperator(
        task_id='end',
        trigger_rule = TriggerRule.ALL_DONE,
        dag=dag
    )
    
    insert_data_task = PythonOperator(
        task_id='insert_into_datamart_test',
        python_callable=generate_insert_query,
        op_kwargs={'project_id': project_id, 'dataset': dataset, 'table_nm': tb_indicator_mart, 'execute_date':ds },
        provide_context=True,
        trigger_rule = TriggerRule.ALL_SUCCESS,
        dag=dag
    )
    
    start >> delete_data_task >>  insert_data_task >> end
    delete_data_task >> failure >> end
