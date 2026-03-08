from datetime import datetime, timedelta
import os
import sys


# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG


default_args={'owner':'savvas',
              'retries':1,
              'retry_delay':timedelta(minutes=1),
              'depends_on_past':False,
              'start_date':datetime(2026,3,8)}

DAGS_DIR=os.path.dirname(__file__)
PROJECT_ROOT=os.path.dirname(DAGS_DIR)

with DAG(
    dag_id='1_bronze_weather_ingestion',
    default_args=default_args,
    schedule='*/10 * * * *',
    catchup=False
    ) as bronze_dag:
    
    task_1=BashOperator(
        task_id='run_bronze',
        bash_command=f'{sys.executable} {os.path.join(PROJECT_ROOT,"Requesting_script.py")}',
        cwd=PROJECT_ROOT
        )
    
    
with DAG(dag_id='2_silver_weather_batching',
    default_args=default_args,
    schedule='0 * * * *',
    catchup=False
    ) as silver_dag:

    task_2=BashOperator(
        task_id='run_silver',
        bash_command=f'{sys.executable} {os.path.join(PROJECT_ROOT,"transform-silver.py")}',
        cwd=PROJECT_ROOT
        )
    

with DAG(dag_id='3_gold_weather_aggregation',
    default_args=default_args,
    schedule='0 0 * * *',
    catchup=False
    ) as gold_dag:
      

        task_3=BashOperator(
            task_id='run_gold',
            bash_command=f'{sys.executable} {os.path.join(PROJECT_ROOT,"transform-gold.py")}',
            cwd=PROJECT_ROOT
            )
    