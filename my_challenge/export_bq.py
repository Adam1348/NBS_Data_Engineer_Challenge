from google.cloud import bigquery
from google.cloud import storage
import random
import sys
import os


def query_execution(client, dataset_id, sql):
    """
    Execute sql query to get the output
    """
    job_config = bigquery.QueryJobConfig()
    table_name = 'temp_' + str(random.randint(1, 10))
    table_ref = client.dataset(dataset_id).table(table_name)
    job_config.destination = table_ref

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=job_config)  # API request - starts the query

    print('Executing query.')
    query_job.result()
    print('Completed.')
    return table_name


def export_to_storage(client, dataset_id, table_name, bucket_name):
    """
    Move the file from bigquery to cloud storage due to export limitation
    """
    table_id = table_name
    destination_filename = os.getlogin() + '_output.csv'
    destination_uri = 'gs://{}/{}'.format(bucket_name, destination_filename)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location='US')  # API request

    extract_job.result()  # Waits for job to complete.

    print('Exported {}.{} to {}'.format(dataset_id, table_id, destination_uri))

    return destination_filename


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """
    Downloads a blob from the bucket.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('{} downloaded to {}.'.format(source_blob_name, destination_file_name))
    return


def remove_table(client, dataset_id, table_id):
    """
    remove the table
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    client.delete_table(table_ref)
    print('Table {}:{} deleted.'.format(dataset_id, table_id))


if __name__ == '__main__':
    input_name = sys.argv[1:][0]
    Client = bigquery.Client()
    data_set_id = 'wiki'
    bucket_name1 = 'pandora-bq-output'

    sql = """
        SELECT
          Language,
          Page,
          Date_time,
          Total_views,
          rank
        FROM (
          SELECT
            Language,
            Page,
            Total_views,
            Date_time,
            ROW_NUMBER() OVER (PARTITION BY Language, Date_time ORDER BY Total_views DESC) AS rank
          FROM
            `arcane-timer-228503.wiki.""" + str(input_name) + """`
          GROUP BY
            Language, Page, Total_views, Date_time
            )
        WHERE
          rank <= 10
        ORDER BY
          Date_time ASC,
          Language ASC,
          Rank ASC
    """

    t_name = query_execution(Client, data_set_id, sql)
    filename = export_to_storage(Client, data_set_id, t_name, bucket_name1)

    download_blob(bucket_name1, filename, os.path.abspath(os.path.join(os.getcwd(), os.pardir)) +
                  '/pandora_nbs_output.csv')
    remove_table(Client, data_set_id, t_name)
