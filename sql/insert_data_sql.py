from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

def insert_mart_data(project_id, dataset, table_nm, execute_date, **context):
    insert_sql = """
        INSERT INTO `{0}.{1}.{2}`
        WITH user_event_log AS ( 
            SELECT year, month, event_date, event_timestamp, week, week_num, platform
                , CASE WHEN platform = 'Web' THEN ''
                    ELSE CAST(app_info.version AS STRING) 
                END AS version
                , user_pseudo_id, event_id, event_name, ga_session_id, params_count, event_params 
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
            WHERE event_date = DATE('{3}')
        )
        , event_log AS (
            SELECT l.year, l.month, l.event_date, l.event_timestamp, l.week, l.week_num
                , l.platform, l.version, l.user_pseudo_id, l.event_id, l.event_name, l.params_count
                , p.key, COALESCE(p.value.string_value, CAST(p.value.int_value AS STRING)) AS value
                , p.value.string_value, p.value.int_value
            FROM user_event_log AS l INNER JOIN UNNEST(event_params) AS p
        )
        , daily_event AS (
            SELECT month, event_date, platform, version
                , COUNT(DISTINCT user_pseudo_id) AS daily_active_count
            FROM user_event_log
            GROUP BY month, event_date, platform, version
            
            UNION ALL

            SELECT month, event_date, platform, 'ALL' AS version
                , COUNT(DISTINCT user_pseudo_id) AS daily_active_count
            FROM user_event_log
            GROUP BY month, event_date, platform

            UNION ALL 

            SELECT month, event_date, 'ALL' AS platform, 'ALL' AS version
                , COUNT(DISTINCT user_pseudo_id) AS daily_active_count
            FROM user_event_log
            GROUP BY month, event_date
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
        SELECT w.year, w.week, d.month, d.event_date, d.platform, d.version
            , d.daily_active_count
            , w.daily_session_count, w.weekly_session_count
            , CURRENT_DATETIME('Asia/Seoul') AS etl_load_dttm
        FROM daily_event AS d 
            LEFT OUTER JOIN weekly_event AS w ON d.event_date = w.event_date
    """.format(project_id, dataset, table_nm, execute_date)
    
    return  BigQueryOperator(
        task_id='insert_mart_data',
        sql=insert_sql,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='bigquery_conn',
        trigger_rule = TriggerRule.ALL_SUCCESS,
        dag=dag
    )
