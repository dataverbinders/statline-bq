from google.cloud import bigquery

gcp = {
    "project": "dataverbinders-dev",
}

client = bigquery.Client(project=gcp["project"])
dataset_id = "dso"  # DATASET NEEDS TO EXIST TODO: Create dataset if doesn't exist?

dataset_ref = bigquery.DatasetReference(gcp["project"], dataset_id)

table_id = "test_from_gcs"

table = bigquery.Table(dataset_ref.table(table_id))

external_config = bigquery.ExternalConfig("PARQUET")
external_config.source_uris = [
    "https://storage.cloud.google.com/dataverbinders-dev_test/cbs/v4/83765NED/20201115/cbs.83765NED_Observations.parquet"
]

table.external_data_configuration = external_config

# Create a permanent table linked to the GCS file
table = client.create_table(table)  # API request

# # Example query to find states starting with 'W'
# sql = f'SELECT * FROM `{dataset_id}.{table_id}` WHERE name LIKE "W%"'
# Example query Select all
sql = f"SELECT * FROM `{dataset_id}.{table_id}` LIMIT 100"
query_job = client.query(sql)

print(query_job.result)
