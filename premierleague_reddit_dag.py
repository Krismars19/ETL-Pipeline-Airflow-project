from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime
from premierleague_reddit_etl import reddit_extract

default_args = {
    'owner' : 'airflow', 
    'depends_on_past': False,
    'start_date' : datetime(2020, 11, 8), 
    'email' : ['airflow@example.com'], 
    'email_on_failure' : False, 
    'email_on_retry' : False,  
    'retries' : 1,  
    'retry_delay' : timedelta(minutes = 1)
}

dag = DAG(
    'reddit_dag',  
    default_args = default_args,  
    description = 'Premier League Reddit Comments with ETL process!',
    schedule_interval=timedelta(days=1)
)

run_etl = PythonOperator(
    task_id = 'complete_PL_reddit_etl', 
    python_callable = reddit_extract,
    dag = dag
    
)

run_etl