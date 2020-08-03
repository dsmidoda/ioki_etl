from google.cloud import bigquery
from google.cloud import storage
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\szef\\Desktop\\creds.json"


def get_bucket():
    storage_client = storage.Client()
    bucket_to_check = "ioki_datasets"
    bucket_name = storage_client.get_bucket(bucket_to_check).exists()
    if bucket_name:
        return "gs://{}".format(bucket_to_check)
    else:
        "Bucket not found. Please create the bucket"


def upload_from_bq_to_storage():
    client = bigquery.Client()
    bucket_name = get_bucket()
    project = 'pyetl01'
    dataset_id = "results"
    table_list = client.list_tables(dataset=dataset_id)
    table_names = [table.table_id for table in table_list]

    try:
        for table in table_names:
            table_id = table
            destination_uri = "{}/{}.csv".format(bucket_name, table_id)
            table_ref = "{}.{}.{}".format(project, dataset_id, table_id)

            extract_job = client.extract_table(table_ref, destination_uri, location="US")
            extract_job.result()

            print(
                "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
                )
    except Exception as e:
        print(e)


if __name__ == '__main__':
    upload_from_bq_to_storage()
