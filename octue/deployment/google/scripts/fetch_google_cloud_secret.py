import logging
import os
import google.auth
from google.cloud import secretmanager_v1


logger = logging.getLogger(__name__)


def fetch_gcloud_secrets(secret, version="latest", env_file=".env"):
    """Fetches the secret from google cloud secrets manager and writes to a local file

    :param str secret: The name of the secret in google cloud Secret Manager
    :param str version: The verison of the secret to use, default "latest"
    :param str env_file: Name of the file to write secrets to, default ".env"
    :return None:
    """
    print(f"Attempting to retrieve environment from google secret {secret}")

    _, project = google.auth.default()

    if project:
        print(f"Authenticated for project {project}")
        client = secretmanager_v1.SecretManagerServiceClient()

        name = f"projects/{project}/secrets/{secret}/versions/{version}"
        payload = client.access_secret_version(name=name).payload.data.decode("UTF-8")

        with open(env_file, "w") as f:
            f.write(payload)
    else:
        print("No gcloud project found, cannot retrieve .env file.")


# If GCLOUD ENV_SECRET is present in the environment, pull it from Google Cloud Secret Manager and store it locally
secret = os.getenv("GCLOUD_ENV_SECRET_NAME", None)
print(f"Check for GCLOUD_ENV_SECRET_NAME environment variable: {secret}")

if secret is not None:
    fetch_gcloud_secrets(secret)
