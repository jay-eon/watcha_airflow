from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'start_date': airflow.utils.dates.days_ago(2),
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'tutorial_bash',
    default_args=default_args,
    description='My first tutorial bash DAG',
    schedule_interval= '* * * * *'
)

t1 = BashOperator(
    task_id='say_hello',
    bash_command='echo "Hello world!"',
    dag=dag
)

t2 = BashOperator(
    task_id='what_time',
    bash_command='date',
    dag=dag
)


t1 >> t2