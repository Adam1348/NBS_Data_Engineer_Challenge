import os
import sys
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import export_bq


def create_dateset(client, dataset_id):
    """
    create the dataset
    """

    # Create a DatasetReference using a chosen dataset ID.
    # The project defaults to the Client's project if not specified.
    dataset_ref = client.dataset(dataset_id)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_ref)
    # Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset)  # API request
    return dataset


def create_table(client, dataset, table_name):
    """
    create a temp table
    """
    # dataset_ref = client.dataset('my_dataset')
    dataset_ref = dataset.reference

    schema = [
        bigquery.SchemaField('Language', 'STRING'),
        bigquery.SchemaField('Page', 'STRING'),
        bigquery.SchemaField('View_count', 'INTEGER'),
        bigquery.SchemaField('Bytes_transferred', 'INTEGER'),
        bigquery.SchemaField('Date_time', 'STRING')
    ]

    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)  # API request

    return table


def dataset_check(client, dataset_name):
    """
    check if the dataset exists
    """
    try:
        client.get_dataset(dataset_name)
        return True
    except NotFound:
        return False


def table_check(client, dataset_id, table_name):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    try:
        table = client.get_table(table_ref)
        if table:
            return True
    except NotFound:
        return False


# def wait_for_job(job):
#     while True:
#         job.reload()
#         if job.state == 'DONE':
#             if job.error_result:
#                 raise RuntimeError(job.errors)
#             return
#         time.sleep(1)


def upload_to_bq(client, table, filename):
    """
    upload the csv(s) to bigquery temp table
    """
    dataset_id = table.dataset_id
    table_id = table.full_table_id

    # dataset_ref = client.dataset(dataset_id)
    # table_ref = dataset_ref.table(table_id)
    table_ref = table.reference
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    # job_config.autodetect = True

    with open(filename, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='US',
            job_config=job_config)  # API request

    print('Loading {}'.format(filename))

    # wait_for_job(job)
    try:
        job.result()  # Waits for table load to complete.
        print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))
    except NotFound:
        print('Failed to upload.')


def query_execution(client, dataset_id, target_name, sql):
    """
    Execute sql query to get the processed data
    """
    job_config = bigquery.QueryJobConfig()
    # table_name = 'temp_' + str(random.randint(1, 10))
    table_ref = client.dataset(dataset_id).table(target_name)
    job_config.destination = table_ref
    job_config.write_disposition = 'WRITE_APPEND'
    
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
    return


if __name__ == '__main__':

    tablename = sys.argv[1:][0]

    Client = bigquery.Client()
    dataset_name = 'wiki'

    if not dataset_check(Client, dataset_name):
        data_set = create_dateset(Client, dataset_name)
    else:
        data_set = Client.get_dataset(Client.dataset('wiki'))

    target = create_table(Client, data_set, 'temp')

    files = os.listdir(os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/wiki_page_view_count')
    for name in files:
        file_name = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/wiki_page_view_count/' + name
        upload_to_bq(Client, target, file_name)

    # if not table_check(Client, dataset_name, tablename):
    #     target = create_table(Client, data_set, tablename)
    # else:
    #     target = Client.get_table(data_set.table(tablename))
        # print('Use existing table.')

    SQL = """
            SELECT
              Language,
              Page,
              SUM(View_count) AS Total_views,
              Date_time  
            FROM `arcane-timer-228503.wiki.temp` 
            GROUP BY Language, Page, Date_time
        """

    query_execution(Client, dataset_name, tablename, SQL)
    export_bq.remove_table(Client, dataset_name, 'temp')
