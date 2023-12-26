#===================================================================================================================
# Extract Transform Load (ETL) STEP
# Author: Gustavo Barreto
# Language: Python
# Date 26/12/2023
#===================================================================================================================
from airflow.hooks.S3_hook import S3Hook

def send_to_s3():

    aws_conn_id = 'AWS_s3'
    bucket_name = 'gustavogrungekk'
    s3_key = 'Datasets/pokemon_dataset.csv'
    local_file_path = '/opt/airflow/datasets/refined/pokemon_dataset.csv' 

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id)

    # Upload file to S3
    s3_hook.load_file(local_file_path, s3_key, bucket_name, replace=True)

    return 'File successfully sent to S3'

send_to_s3()