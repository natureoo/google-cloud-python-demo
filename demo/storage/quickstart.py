from google.cloud import storage
from google.cloud.storage import Bucket
import os.path


client = storage.Client()

#create a new bucket
def test_create_bucket(bucket_name):

    # [START create_bucket]
    bucket = client.create_bucket(bucket_name)
    # assert isinstance(bucket, Bucket)
    # <Bucket: my-bucket>
    return bucket
    # [END create_bucket]




def upload_blob(bucket_name, source_file_name, destination_blob_name,content_type):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name,content_type)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

#upload localfile to the remote bucket
def test_upload_localfile():
    my_path = os.path.abspath(os.path.dirname(__file__))
    source_file_name = os.path.join(my_path, "../../python_sample.csv")
    bucket_name = "tmp-mpp-bucket_1"
    # source_file_name = "../python_sample.csv"
    destination_blob_name = "demo/python_sample.csv"

    upload_blob(bucket_name, source_file_name, destination_blob_name, "text/csv")


if __name__ == '__main__':
    test_create_bucket("tmp-mpp-bucket_1")
    # test_upload_localfile()