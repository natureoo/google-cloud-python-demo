from google.cloud import storage

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
    bucket_name = "test-mpp-bucket"
    source_file_name = "/Users/chenjian/Documents/chenj/work/workplace/study/demo/google-cloud-python-demo/python_sample.csv"
    destination_blob_name = "demo/python_sample.csv"

    upload_blob(bucket_name,source_file_name,destination_blob_name,"text/csv")