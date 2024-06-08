import pendulum
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    "owner": "islamiang",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Задаем параметры и пути к данным
execution_date = '{{ ds }}'
raw_events_hdfs_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/geo/events'
ods_events_hdfs_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/islamiang/data/geo/events'
user_zone_report_hdfs_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/islamiang/data/datamart/user_zone_report'
week_zone_report_hdfs_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/islamiang/data/datamart/week_zone_report'
recommendation_zone_report_hdfs_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/islamiang/data/datamart/recomendation_zone_report'

with DAG(
        '7nth_project_dag',
        default_args=default_args,
        description='ETL',
        catchup=False,
        schedule_interval='0 2 * * *',
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        tags=['pyspark', 'hadoop', 'hdfs', 'datalake', 'geo', 'datamart'],
        is_paused_upon_creation=True,
) as dag:
    start = DummyOperator(task_id='start')
    
    # Мигрируем гео-события
    migrate_geo_events = BashOperator(
        task_id='migrate_geo_events',
        bash_command=f"spark-submit --master local /lessons/dags/scripts/migration_geo_events.py {execution_date} {raw_events_hdfs_path} {ods_events_hdfs_path}",
        retries=5
    )

    # Создаем отчет по зонам пользователей
    build_user_zone_report = BashOperator(
        task_id='build_user_zone_report',
        bash_command=f"spark-submit --master local /lessons/dags/scripts/rpt_user_zone.py {execution_date} {ods_events_hdfs_path} {user_zone_report_hdfs_path}",
        retries=5
    )
    
    # Создаем недельный отчет по зонам
    build_week_zone_report = BashOperator(
        task_id='build_week_zone_report',
        bash_command=f"spark-submit --master local /lessons/dags/scripts/rpt_week_zone.py {execution_date} {ods_events_hdfs_path} {week_zone_report_hdfs_path}",
        retries=5
    )

    # Создаем рек отчет по зонам
    build_recommendation_zone_report = BashOperator(
        task_id='build_recommendation_zone_report',
        bash_command=f"spark-submit --master local /lessons/dags/scripts/rpt_rec_zone.py {execution_date} {ods_events_hdfs_path} {recommendation_zone_report_hdfs_path}",
        retries=5
    )
    
    finish = DummyOperator(task_id='finish')
    
    start >> migrate_geo_events >> [build_user_zone_report, build_week_zone_report, build_recommendation_zone_report] >> finish
