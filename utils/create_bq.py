
# TODO refactor & implement
# Make sure to follow the quickstart setup instructions beforehand.
# See instructions here:
# https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries

# Before running the sample, be sure to install the bigquery library
# in your local environment by running pip install google.cloud.bigquery

from google.cloud import bigquery

# TODO(developer): Replace with your values
project = 'your-project'  # Your GCP Project
location = 'US'  # the location where you want your BigQuery data to reside. For more info on possible locations see https://cloud.google.com/bigquery/docs/locations
dataset_name = 'average_weather'


def create_dataset_and_table(project, location, dataset_name):
    # Construct a BigQuery client object.
    client = bigquery.Client(project)

    dataset_id = f"{project}.{dataset_name}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Set the location to your desired location for the dataset.
    # For more information, see this link:
    # https://cloud.google.com/bigquery/docs/locations
    dataset.location = location

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset)  # Make an API request.

    print(f"Created dataset {client.project}.{dataset.dataset_id}")

    # Create a table from this dataset.

    table_id = f"{client.project}.{dataset_name}.average_weather"

    schema = [
        bigquery.SchemaField("location", "GEOGRAPHY", mode="REQUIRED"),
        bigquery.SchemaField("average_temperature", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("month", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("inches_of_rain", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("is_current", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("latest_measurement", "DATE", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")