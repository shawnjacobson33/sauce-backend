from google.cloud import storage


class GCSUploader:

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name  # Todo: set to env variable
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)

    def upload(self, blob_name: str, json_data: str) -> None:
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(json_data)



