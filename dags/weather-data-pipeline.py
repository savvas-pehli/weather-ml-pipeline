from datetime import datetime, timedelta
import os
import sys


# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG


default_args={'owner':'savvas',
              'retries':1,
              'retry_delay':timedelta(minutes=1)}

DAGS_DIR=os.path.dirname(__file__)
PROJECT_ROOT=os.path.dirname(DAGS_DIR)

with DAG(
    dag_id='weather_medallion_pipeline',
    default_args=default_args,
    schedule='0 10,14 * * 1-5',
    start_date=datetime(2024,1,1),
    catchup=False
    ) as dag:
    
    task_1=BashOperator(task_id='run_bronze',
                        bash_command=f'{sys.executable} {os.path.join(PROJECT_ROOT,"Requesting_script.py")}',
                        cwd=PROJECT_ROOT)
    
    task_2=BashOperator(
        task_id='run_silver',
        bash_command=f'{sys.executable} {os.path.join(PROJECT_ROOT,"transform-silver.py")}',
        cwd=PROJECT_ROOT     
        )
    
    task_3=BashOperator(
        task_id='run_gold',
        bash_command=f'{sys.executable} {os.path.join(PROJECT_ROOT,"transform-gold.py")}',
        cwd=PROJECT_ROOT
    )
    task_1>>task_2>>task_3
    