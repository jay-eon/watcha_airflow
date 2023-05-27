from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
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

# DAG 시작 (오전 7시마다 수행)
with DAG(
        'daily_indicator_mart', 
        default_args=default_args, 
        schedule_interval='0 7 * * * *',
        catchup=False
    ) as dag:
    
    yesterday_ds = '{{ yesterday_ds }}'
    ds = '{{ ds }}'
    tomorrow_ds = '{{ tomorrow_ds }}'
    
    delete_query = """
        DELETE FROM `{0}.{1}.{2}`
        WHERE event_date = DATE('{3}')
    """.format(project_id, dataset, tb_indicator_mart, ds)
    
    insert_query = """
        INSERT INTO `{0}.{1}.{2}`
        WITH user_event_log AS ( 
            SELECT year, month, event_date, event_timestamp, week, week_num, platform, app_info.version, user_pseudo_id, event_id, event_name, ga_session_id, params_count, event_params 
            FROM (
                SELECT *, DATE(event_timestamp, 'Asia/Seoul') AS event_date
                    , EXTRACT(YEAR FROM DATE(event_timestamp, 'Asia/Seoul')) AS year
                    , EXTRACT(MONTH FROM DATE(event_timestamp, 'Asia/Seoul')) AS month
                    , EXTRACT(WEEK FROM DATE(event_timestamp, 'Asia/Seoul')) AS week
                    , EXTRACT(DAYOFWEEK FROM DATE(event_timestamp, 'Asia/Seoul')) AS week_num
                    , IF(event_params[SAFE_OFFSET(0)].key ='event_id', event_params[SAFE_OFFSET(0)].value.int_value, NULL) AS event_id
                    , event_params[SAFE_OFFSET(ARRAY_LENGTH(event_params)-1)].value.int_value AS ga_session_id
                    , ARRAY_LENGTH(event_params) AS params_count 
                FROM `seventh-coast-386809.watcha.user_event_log` 
            )
            WHERE event_date = '{3}'
        )
        , event_log AS (
            SELECT l.year, l.month, l.event_date, l.event_timestamp, l.week, l.week_num
                , l.platform, l.version, l.user_pseudo_id, l.event_id, l.event_name, l.params_count
                , p.key, COALESCE(p.value.string_value, CAST(p.value.int_value AS STRING)) AS value
                , p.value.string_value, p.value.int_value
            FROM user_event_log AS l INNER JOIN UNNEST(event_params) AS p
        )
        , daily_event AS (
            SELECT DISTINCT month, event_date, platform, version
                , COUNT(DISTINCT user_pseudo_id) OVER(PARTITION BY event_date) AS daily_active_user
                , COUNT(DISTINCT user_pseudo_id) OVER(PARTITION BY event_date, platform, CAST(version AS STRING)) AS daily_active_device
            FROM user_event_log
        )
        , weekly_event AS (
            SELECT DISTINCT year, event_date, week
                , SUM(user_session_count) OVER(PARTITION BY event_date) AS daily_session_count
                , SUM(user_session_count) OVER(PARTITION BY year, week) AS weekly_session_count
            FROM (
                SELECT year, event_date, week, user_pseudo_id
                    , COUNT(DISTINCT int_value) AS user_session_count
                FROM event_log
                WHERE key = 'ga_session_id'
                GROUP BY year, event_date, week, user_pseudo_id
            )
        )
        , event_info AS (
            SELECT *
                , ARRAY_LENGTH(SPLIT(params, ','))+1 AS params_count
            FROM `{0}.{1}.{4}`
        )
        , wrong_event_log AS (
            SELECT event_date, platform, version, log_count, wrong_log_count
                , ROUND((wrong_log_count / log_count),2) AS wrong_log_ratio
                , ROUND((wrong_log_count / log_count),2) * 100  AS wrong_log_percent
            FROM (
                SELECT DISTINCT event_date, platform, version
                    , COUNT(1) AS log_count
                    , COUNT(IF((l.event_id = e.event_id AND l.params_count <> e.params_count) OR l.event_id IS NULL, 1 , NULL)) AS wrong_log_count
                FROM user_event_log AS l, event_info AS e
                GROUP BY event_date, platform, version
            )
        )
        SELECT w.year, w.week, d.month, d.event_date, d.platform, d.version
            , d.daily_active_user, d.daily_active_device
            , e.log_count, e.wrong_log_count, e.wrong_log_ratio, e.wrong_log_percent
            , w.daily_session_count, w.weekly_session_count
            , DATETIME(CURRENT_TIMESTAMP(), 'Asia/Seoul')
        FROM daily_event AS d 
            LEFT OUTER JOIN wrong_event_log AS e ON d.event_date = e.event_date 
                AND d.platform = e.platform 
                AND IF(d.platform <>'Web', d.version = e.version, d.platform = e.platform)
            LEFT OUTER JOIN weekly_event AS w ON d.event_date = w.event_date
    """.format(project_id, dataset, tb_indicator_mart, ds, tb_event_info)
    
    delete_duplicates_task = BigQueryOperator(
        task_id='delete_mart_data',
        sql=delete_query,
        use_legacy_sql=False,
        bigquery_conn_id='bigquery_conn',
        dag=dag
    )
    
    failure = DummyOperator(
        task_id='failure',
        trigger_rule = 'all_failed',
        dag=dag
    )
    
    start = DummyOperator(
        task_id='start',
        dag=dag
    )
    
    end = DummyOperator(
        task_id='end',
        trigger_rule = 'always',
        dag=dag
    )
    
    insert_data_task = BigQueryOperator(
        task_id='insert_mart_data',
        sql=insert_query,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='bigquery_conn',
        trigger_rule = 'all_success',
        dag=dag
    )
    
    start >> delete_duplicates_task >> insert_data_task >> end
    delete_duplicates_task >> failure >> end