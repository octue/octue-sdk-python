import google.auth
import google.auth.transport.requests


def get_gcp_credentials():
    """Get the default credentials for Google Cloud Platform."""
    credentials, _ = google.auth.default()
    auth_request = google.auth.transport.requests.Request()
    credentials.refresh(auth_request)
    return credentials
