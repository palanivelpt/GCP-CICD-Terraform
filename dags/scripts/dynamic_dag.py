import yaml
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import requests

# Function to load YAML configuration
def load_config():
    with open('path/to/config.yaml', 'r') as file:
        return yaml.safe_load(file)

# Task to call API and load data to BigQuery
def api_call_and_load_data(api_endpoint, target_dataset, destination_table, **kwargs):
    # Make API call
    response = requests.get(api_endpoint)
    data = response.json()
    
    # Load data into BigQuery
    client = bigquery.Client()
    dataset_ref = client.dataset(target_dataset)
    table_ref = dataset_ref.table(destination_table)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_json(data, table_ref, job_config=job_config)

# Task to move data from landing to staging dataset
def move_data_to_staging(landing_dataset, staging_dataset, table, **kwargs):
    client = bigquery.Client()
    query = f"""
        INSERT INTO `{staging_dataset}.{table}`
        SELECT * FROM `{landing_dataset}.{table}`
    """
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete

# Load config file
config = load_config()

# Create dynamic DAGs based on config
for dag_config in config['dags']:
    dag_id = dag_config['dag_id']
    schedule_interval = dag_config['schedule_interval']
    api_endpoint = dag_config['api_endpoint']
    target_dataset = dag_config['target_dataset']
    destination_table = dag_config['destination_table']

    # Define the DAG
    default_args = {
        'start_date': days_ago(1),
    }
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False
    )

    # Task 1: Call API and load data to BigQuery landing dataset
    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=api_call_and_load_data,
        op_kwargs={
            'api_endpoint': api_endpoint,
            'target_dataset': target_dataset,
            'destination_table': destination_table
        },
        dag=dag,
    )

    # Task 2: Move data from landing to staging
    move_data_task = PythonOperator(
        task_id='move_data_to_staging_task',
        python_callable=move_data_to_staging,
        op_kwargs={
            'landing_dataset': target_dataset,
            'staging_dataset': 'your_staging_dataset_name',  # Replace with your staging dataset name
            'table': destination_table
        },
        dag=dag,
    )

    # Set task dependencies
    load_data_task >> move_data_task

    # Register the DAG in globals to make it available in Airflow
    globals()[dag_id] = dag
