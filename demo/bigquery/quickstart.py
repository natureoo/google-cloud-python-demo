#!/usr/bin/env python

# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import requests

from google.cloud import bigquery
from IPython.display import display


client = bigquery.Client()

def test_list_datasets():
    """List datasets for a project."""
    # [START bigquery_list_datasets]

    print("Datasets list")
    datasets = list(client.list_datasets())
    print(datasets)
    # project = client.project
    #
    # if datasets:
    #     print("Datasets in project {}:".format(project))
    #     for dataset in datasets:  # API request(s)
    #         print("\t{}".format(dataset.dataset_id))
    # else:
    #     print("{} project does not contain any datasets.".format(project))
    # [END bigquery_list_datasets


def run_quickstart():
    # [START bigquery_quickstart]
    # Imports the Google Cloud client library

    # Instantiates a client
    # client = bigquery.Client()

    # The name for the new dataset
    dataset_id = 'my_new_dataset'

    # Prepares a reference to the new dataset
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)

    # Creates the new dataset
    dataset = client.create_dataset(dataset)

    print('Dataset {} created.'.format(dataset.dataset_id))
    # [END bigquery_quickstart]

def test_list_datasets():
    """List datasets for a project."""
    # [START bigquery_list_datasets]
    # from google.cloud import bigquery
    # client = bigquery.Client()

    datasets = list(client.list_datasets())
    project = client.project

    if datasets:
        print("Datasets in project {}:".format(project))
        for dataset in datasets:  # API request(s)
            print("\t{}".format(dataset.dataset_id))
    else:
        print("{} project does not contain any datasets.".format(project))
    # [END bigquery_list_datasets]

def test_list_storages():
    from google.cloud import storage

    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    storage_client = storage.Client()

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)

# def auth():

def test_create_dataset(dataset_id):
    """Create a dataset."""
    client = bigquery.Client()

    # [START bigquery_create_dataset]
    # from google.cloud import bigquery
    # client = bigquery.Client()
    # dataset_id = 'my_dataset'

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
    # [END bigquery_create_dataset]

# [START bigquery_dataset_exists]
def dataset_exists(dataset_reference):
    """Return if a dataset exists.

    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        dataset_reference (google.cloud.bigquery.dataset.DatasetReference):
            A reference to the dataset to look for.

    Returns:
        bool: ``True`` if the dataset exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False

def test_dataset_exists():
    """Determine if a dataset exists."""
    DATASET_ID = "my_python_dataset"
    dataset_ref = client.dataset(DATASET_ID)

    flags = dataset_exists( dataset_ref)
    print(flags)

# bigquery.SourceFormat.CSV
def test_load_table_from_uri_autodetect(dataset_id,table_name,uri, format):



    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    # [END bigquery_load_table_gcs_csv_autodetect]
    # [END bigquery_load_table_gcs_json_autodetect]

    # Format-specific code
    # [START bigquery_load_table_gcs_csv_autodetect]
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = format
    # uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
    # [END bigquery_load_table_gcs_csv_autodetect]
    # unset csv-specific attribute
    del job_config._properties["load"]["skipLeadingRows"]

    # [START bigquery_load_table_gcs_json_autodetect]
    # job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    # uri = "gs://cloud-samples-data/bigquery/us-states/us-states.json"
    # [END bigquery_load_table_gcs_json_autodetect]

    # Shared code
    # [START bigquery_load_table_gcs_csv_autodetect]
    # [START bigquery_load_table_gcs_json_autodetect]
    load_job = client.load_table_from_uri(
        uri, dataset_ref.table(table_name), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(table_name))
    print("Loaded {} rows.".format(destination_table.num_rows))
    # [END bigquery_load_table_gcs_csv_autodetect]
    # [END bigquery_load_table_gcs_json_autodetect]



def test_client_query_job(dataset_id, table_id):

    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config.destination = table_ref
    job_config.use_legacy_sql = True
    job_config.allow_large_results = True


    sql = "SELECT * FROM python_sample_dataset.python_sample;"

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        # location="US",
        job_config=job_config,
    )  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print("Query results loaded to table {}".format(table_ref.path))

    for row in query_job:  # API request - fetches results
        # Row values can be accessed by field name or index
        # assert row[0] == row.name == row["name"]
        print(row)
    # [END bigquery_query]

def test_query_results_sql():
    # [START bigquery_query_results_dataframe]
    # from google.cloud import bigquery
    # client = bigquery.Client()

    sql = """
     SELECT * FROM python_sample_dataset.python_sample;
    """

    query_job = client.query(sql)
    for row in query_job:  # API request - fetches results
        # Row values can be accessed by field name or index
        # assert row[0] == row.name == row["name"]
        print(row)
    # df = client.query(sql).to_dataframe()
    # [END bigquery_query_results_dataframe]


def test_query_results_dataframe(sql):
    # [START bigquery_query_results_dataframe]
    # from google.cloud import bigquery
    # client = bigquery.Client()

    df = client.query(sql).to_dataframe()
    # print(df)
    display(df)

    # df.plot()
    # [END bigquery_query_results_dataframe]


def test_client_list_jobs():
    """List jobs for a project."""

    # [START bigquery_list_jobs]
    # TODO(developer): Uncomment the lines below and replace with your values.
    # from google.cloud import bigquery
    # project = 'my_project'  # replace with your project ID
    # client = bigquery.Client(project=project)
    import datetime

    # List the 10 most recent jobs in reverse chronological order.
    # Omit the max_results parameter to list jobs from the past 6 months.
    print("Last 10 jobs:")
    for job in client.list_jobs(max_results=10):  # API request(s)
        print(job.job_id, job.job_type, job.errors)

    # The following are examples of additional optional parameters:

    # # Use min_creation_time and/or max_creation_time to specify a time window.
    # print("Jobs from the last ten minutes:")
    # ten_mins_ago = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
    # for job in client.list_jobs(min_creation_time=ten_mins_ago):
    #     print(job.job_id)

    # # Use all_users to include jobs run by all users in the project.
    # print("Last 10 jobs run by all users:")
    # for job in client.list_jobs(max_results=10, all_users=True):
    #     print("{} run by user: {}".format(job.job_id, job.user_email))

    # # Use state_filter to filter by job state.
    # print("Jobs currently running:")
    # for job in client.list_jobs(state_filter="RUNNING"):
    #     print(job.job_id)
    # # [END bigquery_list_jobs]

def test_codes():
    try:
        page1 = requests.get("https://www.youtube.com")
        print(page1)
    except Exception as ex:
         print(ex)


if __name__ == '__main__':
    # test_create_dataset("python_sample_dataset_1")


    test_load_table_from_uri_autodetect("python_sample_dataset_1", "python_sample_mpp",  "gs://tmp-mpp-bucket_1/demo/python_sample.csv", bigquery.SourceFormat.CSV)
    sql = """
       SELECT * FROM python_sample_dataset_1.python_sample_mpp;
      """
    test_query_results_dataframe(sql)