from google.cloud import storage
import os.path


client = storage.Client()


# [START storage_upload_file]
def upload_blob(bucket_name, source_file_name, destination_blob_name,content_type):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name,content_type)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))




if __name__ == '__main__':
    my_path = os.path.abspath(os.path.dirname(__file__))
    source_file_name = os.path.join(my_path, "../../python_sample.csv")
    bucket_name = "test-mpp-bucket"
    # source_file_name = "../python_sample.csv"
    destination_blob_name = "demo/python_sample_1.csv"

    upload_blob(bucket_name,source_file_name,destination_blob_name,"text/csv")