from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from google.cloud import storage


gcp_project = "pyetl01"
bucketname = "ioki_datasets"
bq_dataset_id = "RawDataset"
result_dataset = "results"


def storage_files(bucket):
    client = storage.Client()
    bucket_name = bucket
    blobs = client.get_bucket(bucket_name).list_blobs()
    blob_path = ["gs://{}/{}".format(bucket_name, blob.name) for blob in blobs]
    return blob_path


def get_bucket_name(bucket):
    storage_client = storage.Client()
    bucket_to_check = bucket
    bucket_name = storage_client.get_bucket(bucket_to_check).exists()
    if bucket_name:
        return "gs://{}".format(bucket_to_check)
    else:
        "Bucket not found. Please create the bucket"


def upload_into_bq(data, dataset):
    try:
        filename = data
        dataset_id = dataset
        table_id = data.split("/")[-1][:-4]
        client = bigquery.Client()
        print("Uploading file {} into table {}.".format(data, table_id))

        dataset_ref = bigquery.DatasetReference(project=gcp_project, dataset_id=dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        load_job = client.load_table_from_uri(filename, table_ref, job_config=job_config)
        load_job.result()

    except Exception as e:
        print(e)


def results_datasets():
    try:
        dataset_id = [bq_dataset_id, result_dataset]
        client = bigquery.Client()

        datasets = list(client.list_datasets())
        datasets_names = [dataset.dataset_id for dataset in datasets]

        for dataset in dataset_id:
            if dataset not in datasets_names:
                client.create_dataset(dataset)
            else:
                print("Dataset already created")
    except Exception as e:
        print(e)


def upload_from_bq_to_storage():
    client = bigquery.Client()
    bucket_name = get_bucket_name(bucketname)
    project = gcp_project
    dataset_id = result_dataset
    table_list = client.list_tables(dataset=dataset_id)
    table_names = [table.table_id for table in table_list]

    try:
        for table in table_names:
            table_id = table
            destination_uri = "{}/{}.csv".format(bucket_name, table_id)
            table_ref = "{}.{}.{}".format(project, dataset_id, table_id)

            extract_job = client.extract_table(table_ref, destination_uri)
            extract_job.result()

            print(
                "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
            )
    except Exception as e:
        print(e)


def run():
    try:
        file_paths = storage_files(bucketname)
        for file in file_paths:
            upload_into_bq(file, bq_dataset_id)
    except Exception as e:
        print(e)


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'project_id': 'pyetl01',
    'region': 'us-central1'
}

dag = DAG("ioki_etl_pipeline", default_args=default_args, max_active_runs=1, concurrency=1)

create_result_dataset = PythonOperator(
    task_id='create_result_dataset',
    python_callable=results_datasets,
    dag=dag)


import_from_storage = PythonOperator(
    task_id='import_data_to_BQ',
    python_callable=run,
    dag=dag)


test_utilization = BigQueryOperator(
    task_id="create_test_utilization_tbl",
    sql="/templates/sql/task3.sql",
    use_legacy_sql=False,
    dag=dag)

average_score = BigQueryOperator(
    task_id="create_test_average_scores_tbl",
    sql="/templates/sql/task4.sql",
    use_legacy_sql=False,
    dag=dag)


save_results = PythonOperator(
    task_id='save_results_to_storage',
    python_callable=upload_from_bq_to_storage,
    dag=dag)

create_result_dataset >> import_from_storage >> test_utilization >> average_score >> save_results
