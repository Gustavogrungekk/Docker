#===================================================================================================================
# Extract Transform Load (ETL) STEP
# Author: Gustavo Barreto
# Language: Python
# Date 26/12/2023
#===================================================================================================================

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import boto3

# Function to send file to AWS S3
default_args = {
    'owner': 'Gustavo Barreto',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email': ['youremail@yum.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

# Create a DAG
dag = DAG(
    dag_id ='pokemon_pipeline',
    description = 'Fetches data from the pokemon API',
    start_date = datetime(2023,12,24, 2),
    schedule_interval = '0 0 * * *',  # Set your desired interval
    catchup = False,
    default_args = default_args
)

# Defining tasks
mkdir_raw = BashOperator(
    task_id = 'creating_directory_raw',
    bash_command = '''
                directory="/opt/airflow/datasets/raw"
                if [ ! -d "$directory" ]; then
                    mkdir -p "$directory"
                    echo "Directory created."
                else
                    echo "Directory already exists."
                fi
                ''',
    dag = dag
    )

mkdir_refined = BashOperator(
    task_id = 'creating_directory_refined',
    bash_command = '''
                directory="/opt/airflow/datasets/refined"
                if [ ! -d "$directory" ]; then
                    mkdir -p "$directory"
                    echo "Directory created."
                else
                    echo "Directory already exists."
                fi
                ''',
    dag = dag
    )

copy = BashOperator(
    task_id = 'copying_files_tmp',
    bash_command = 'cp /opt/airflow/dags/scripts/ /tmp/ -r',
    dag = dag
)

ingest = BashOperator(
    task_id = 'data_ingestion_step',
    bash_command = 'python3 /tmp/scripts/pokemon_data_request.py',
    dag = dag
)

etl_step = BashOperator(
    task_id = 'data_etl_step',
    bash_command = 'python3 /tmp/scripts/pokemon_etl_job.py',
    dag = dag
)

send_s3 = BashOperator(
    task_id = 'sending_dataset_to_s3',
    bash_command = 'python3 /tmp/scripts/send_s3.py',
    dag = dag
)

delete = BashOperator(
    task_id = 'deleting_files_tmp',
    bash_command = 'rm -r /tmp/scripts',
    dag = dag)

notification = BashOperator(
    task_id = 'notifying_end',
    bash_command = 'echo Hey I am all done for now! Great job team!',
    dag = dag)

[mkdir_raw, mkdir_refined ,copy] >> ingest >> etl_step >> [notification, send_s3] >> delete