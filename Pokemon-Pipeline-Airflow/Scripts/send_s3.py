#===================================================================================================================
# Extract Transform Load (ETL) STEP
# Author: Gustavo Barreto
# Language: Python
# Date 26/12/2023
#===================================================================================================================
from airflow.hooks.S3_hook import S3Hook

def send_to_s3(bucket_name=str, local_file_path=str, s3_key=str):
    '''
    bucket_name: Name of the bucket we'll be sending to.
    local_file_path: Location to the file you want to upload to s3.
    s3_key: Prefix location where the file will be stored in of the bucket.
    '''

    aws_conn_id = 'AWS_s3'

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id)

    # Upload file to S3
    s3_hook.load_file(local_file_path, s3_key, bucket_name, replace=True)

    return 'File successfully sent to S3'

# S3 Raw
send_to_s3(
        bucket_name = 'bucket',
        s3_key = 'Datasets/Pokemon/Bronze/pokemon_dataset.csv',
        local_file_path = '/opt/airflow/datasets/raw/raw_pokemon_dataset.csv'
            )

# S3 Refined
send_to_s3(
        bucket_name = 'bucket',
        s3_key = 'Datasets/Pokemon/Silver/pokemon_dataset.csv',
        local_file_path = '/opt/airflow/datasets/refined/pokemon_dataset.csv'
            )