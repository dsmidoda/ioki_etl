# Snowflake Data Insertion
This script loads the data from Cloud Storage to Snowflake Cloud Data Warehouse.
In order to estabilish a connection within the config file update the following:

    user = user_name
    password = user_password
    account = account_link
    warehouse = warehouse_of_your_choice
    database = db_name
    role = account_role
    
This script can also be runned from a containers. In order to do it in GCP
create the container in GKE, build the Docker image of a script, create the POD job.