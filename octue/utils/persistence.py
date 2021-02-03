import logging
from google.cloud import storage

from octue.resources.communication.credentials import GCPCredentialsManager


logger = logging.getLogger(__name__)


def upload_file_to_google_cloud(local_path, project_name, bucket_name, remote_path):
    """Upload a local file to a Google Cloud bucket at <project_name>/<bucket_name>/<remote_path>."""
    client = storage.Client(project=project_name, credentials=GCPCredentialsManager().get_credentials())
    bucket = client.get_bucket(bucket_or_name=bucket_name)
    bucket.blob(blob_name=remote_path).upload_from_filename(filename=local_path)
    logger.info("Uploaded %s to Google cloud at %s/%s/%s.", local_path, project_name, bucket_name, remote_path)
