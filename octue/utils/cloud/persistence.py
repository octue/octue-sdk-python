import logging
from google.cloud import storage

from octue.utils.cloud.credentials import GCPCredentialsManager


logger = logging.getLogger(__name__)


def upload_file_to_google_cloud(local_path, project_name, bucket_name, path_in_bucket):
    """Upload a local file to a Google Cloud bucket at <project_name>/<bucket_name>/<remote_path>."""
    client = storage.Client(project=project_name, credentials=GCPCredentialsManager().get_credentials())
    bucket = client.get_bucket(bucket_or_name=bucket_name)
    bucket.blob(blob_name=path_in_bucket).upload_from_filename(filename=local_path)
    logger.info("Uploaded %s to Google cloud at gs://%s/%s.", local_path, bucket_name, path_in_bucket)
