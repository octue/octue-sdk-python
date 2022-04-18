import datetime
import io

from google.cloud.storage.blob import _API_ACCESS_ENDPOINT


class MockOpen:
    """A mock for patching `builtins.open` that returns different text streams depending on the path given to it. To
    set these, override the class variable `path_to_contents_mapping` with a dictionary mapping the paths to the
    desired output.

    :param str path:
    :param kwargs: any kwargs that the builtin `open` supports
    :return None:
    """

    path_to_contents_mapping = {}

    def __init__(self, path, **kwargs):
        self.__dict__ = {**kwargs}
        self.path = path

    def __enter__(self):
        return io.StringIO(self.path_to_contents_mapping[self.path])

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def mock_generate_signed_url(blob, expiration=datetime.timedelta(days=30), **kwargs):
    """Mock generating a signed URL for a Google Cloud Storage blob. Signed URLs can't currently be generated when using
    workload identity federation, which we use for our CI tests.

    :param google.cloud.storage.blob.Blob blob:
    :param datetime.datetime|datetime.timedelta expiration:
    :return str:
    """
    mock_signed_query_parameter = (
        f"?Expires={round((datetime.datetime.now() + expiration).timestamp())}&GoogleAccessId=my-service-account%40my-p"
        f"roject.iam.gserviceaccount.com&Signature=UekBIUZIgjZKB8aRSTEIbj3QXDXm5fhcEEhRneTKBJoyU7ysnhEmCXiS2Ip5rKwzBS56"
        f"8aFeWJQXbDPSf9Qq43N0%2FHB7QkEwJ6Y5u9S%2FTp6l5%2FqhrKHPhPCSjbJ7gmoaksHpfDaDVVEMQTRA%2Bcq59SV2NBRsA00Ek8h73sBNP"
        f"vw9JlcwNGn9gjbCUunt24ZRVvf3DEThYUZyv7Z2vInv5cbmZYbFd6bA8ahy%2FLFdq%2F6vQibao4iOJ1yeBZKEAaLsYzmXRuZJmg19LWWNBT"
        f"siAiZqKLy%2Fn5fw6LCRAR%2B04GaL8kVpotN1sOh7tRBFedEqoJ3fAXnztdhlJZs2m4OFLg%3D%3D"
    )

    base_url = "/".join((kwargs.get("api_access_endpoint", _API_ACCESS_ENDPOINT), blob.bucket.name, blob.name))
    return base_url + mock_signed_query_parameter
