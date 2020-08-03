from google.cloud import bigquery


def main():
    try:
        dataset_id = "results"
        client = bigquery.Client()

        datasets = list(client.list_datasets())
        datasets_names = [dataset.dataset_id for dataset in datasets]

        if dataset_id not in datasets_names:
            client.create_dataset(dataset_id)
        else:
            print("Dataset already created")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
