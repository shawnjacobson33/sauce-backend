from dotenv import load_dotenv
import pytest
from google.cloud import storage
from db.collections.utils import GCSUploader

load_dotenv()


@pytest.mark.integration
def test_gcs_uploader_integration():
    """Test the GCSUploader integration with real GCS."""
    bucket_name = "betting-lines"

    # Initialize the GCSUploader
    uploader = GCSUploader(bucket_name=bucket_name)

    # Test data
    blob_name = "test_blob"
    json_data = '{"key": "value"}'

    try:
        # Call the upload method
        uploader.upload(blob_name=blob_name, json_data=json_data)

        # Verify the upload by fetching the blob
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.get_blob(blob_name)

        assert blob is not None, "Blob was not uploaded successfully"
        assert blob.download_as_text() == json_data, "Blob content does not match"

    finally:
        # Clean up: delete the test blob
        bucket = uploader.bucket
        blob = bucket.blob(blob_name)
        blob.delete()
