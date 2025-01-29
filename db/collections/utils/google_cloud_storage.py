from google.cloud import storage


class GCSUploader:
    """
    A class for uploading data to Google Cloud Storage.

    Attributes:
        bucket_name (str): The name of the GCS bucket.
        client (storage.Client): The GCS client.
        bucket (storage.Bucket): The GCS bucket object.
    """

    def __init__(self, bucket_name):
        """
        Initializes the GCSUploader with the specified bucket name.

        Args:
            bucket_name (str): The name of the GCS bucket.
        """
        self.bucket_name = bucket_name  # Todo: set to env variable
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)

    def upload(self, blob_name: str, json_data: str) -> None:
        """
        Uploads JSON data to a specified blob in the GCS bucket.

        Args:
            blob_name (str): The name of the blob to upload the data to.
            json_data (str): The JSON data to upload.

        Raises:
            Exception: If there is an error during the upload process.
        """
        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(json_data)

        except Exception as e:
            raise Exception(f'upload(): Failed to upload to GCS: {e}')

