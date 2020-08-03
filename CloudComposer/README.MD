# Cloud Composer
In order to create Cloud Composer Environment run the following command:
    
    gcloud composer environments create test-environment \
    --location us-central1 \
    --zone us-central1-c \
    --machine-type n1-standard-2
    
## Deploying pipeline
Update variables such as project, bucket where raw files are stored, dataset names if you wish. 
While having that upload the pipeline and its dependencies to the dags folder of the Composer service.

Note that before deploying a pipeline it is expected to have files already within the bucket. You can upload files using
fileStorage.py script from CloudStorage directory.