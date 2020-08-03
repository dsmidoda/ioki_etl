# Upload files to Storage
If you do not want to manually create the bucket and upload files run fileStorage script. 
This script automatically create a bucket for you, clean the files of NA values and upload it into GCP Storage.

## Connect to GCP
In order to run operations on GCP provide the credentials or authenticate your session login. You can provide generated 
credentials in the following form:

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "PATH_TO_FILE_WITH_CREDS"
    
or authenticate session from terminal using:

    gcloud auth login