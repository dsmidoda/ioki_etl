from google.cloud import bigquery
from google.cloud import storage


def storage_files():
    client = storage.Client()
    bucket_name = "ioki_datasets"
    blobs = client.get_bucket(bucket_name).list_blobs()
    blob_path = ["gs://{}/{}".format(bucket_name, blob.name) for blob in blobs]
    return blob_path


def upload_into_bq(data):
    try:
        filename = data
        dataset_id = "RawDataset"
        table_id = data.split("/")[-1][:-4]
        client = bigquery.Client()
        print("Uploading file {} into table {}.".format(data, table_id))

        datasets = list(client.list_datasets())
        datasets_names = [dataset.dataset_id for dataset in datasets]

        if dataset_id not in datasets_names:
            client.create_dataset(dataset_id)

        dataset_ref = bigquery.DatasetReference(project='pyetl01', dataset_id=dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        load_job = client.load_table_from_uri(filename, table_ref, job_config=job_config)
        load_job.result()

    except Exception as e:
        print(e)


if __name__ == '__main__':
    file_paths = storage_files()
    for file in file_paths:
        upload_into_bq(file)
