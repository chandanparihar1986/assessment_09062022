import airflow
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    'owner': 'cparihar',
    'start_date': datetime(2022, 7, 11),
    'retries': 3,
    'retry_delay': timedelta(hours=1)
}
# DAG is scheduled to run at 12:00 AM daily
with airflow.DAG('dag_user_events',
                 default_args=default_args,
                 schedule_interval='0 0 * * *') as dag:
    task_elt_user_event = SparkSubmitOperator(
        task_id='user_event_submitter',
        conn_id='spark',
        application="assessment_09062022/target/user_events-1.0-SNAPSHOT-jar-with-dependencies.jar",
        application_args=["config/config.properties"],
        total_executor_cores=2,
        executor_memory="2g",
        conf =   {

                    "spark.network.timeout": 10000000,
                    "spark.executor.heartbeatInterval": 10000000,
                    "spark.storage.blockManagerSlaveTimeoutMs": 10000000,
                    "spark.driver.maxResultSize": "20g"
                },
)