from google.cloud import storage
from google.cloud.storage import Blob
import logging
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\szef\\Desktop\\creds.json"


def create_bucket(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.create_bucket(bucket_name)
    print("Bucket {} created".format(bucket.name))


def upload_files(csvfile, bucketname, dest_blobname):
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    blob = Blob(dest_blobname, bucket)
    blob.upload_from_filename(csvfile, content_type="text/csv")
    gcs_location = 'gs://{}/{}'.format(bucketname, dest_blobname)
    logging.info("Blob {} has been uploaded to {}".format(dest_blobname, gcs_location))
    return gcs_location


def remove_nan(file):
    try:
        import pandas as pd
        df = pd.read_csv(file, sep=";")
        df = df.dropna()
        return df.to_csv("clean_{}".format(file))

    except Exception as e:
        print(e)


def remove_uploaded_files(file):
    return os.remove(file)


def main():
    try:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
        bucketname = "ioki_datasets"
        filepaths = ['test.csv', 'class.csv', 'test_level.csv']

        storage_client = storage.Client()
        if storage_client.bucket(bucketname).exists():
            blobs = storage_client.get_bucket(bucketname).list_blobs()
            blob_names = [blob.name for blob in blobs]
            for file in filepaths:
                remove_nan(file)
                if "clean_{}".format(file) not in blob_names:
                    upload_files("clean_{}".format(file), bucketname, "clean_{}".format(file))
                else:
                    logging.info("File clean_{} already in the bucket".format(file))
                remove_uploaded_files("clean_{}".format(file))
        else:
            create_bucket(bucketname)
            for file in filepaths:
                file = remove_nan(file)
                upload_files(file, bucketname, file)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
    logging.info("Operation completed")
