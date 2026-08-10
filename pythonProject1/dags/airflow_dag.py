from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG('pyspark_example',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False)

run_pyspark_job = BashOperator(
    task_id='run_pyspark_job',
    bash_command='spark-submit /Users/VISHAL/PycharmProjects/pythonProject1/week_9_q1_aasignment_pyspark.py',
    dag=dag
)
