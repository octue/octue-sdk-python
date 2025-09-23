import importlib.metadata
import os

GOOGLE_COMPUTE_PROVIDERS = {"GOOGLE_CLOUD_FUNCTION"}
LOCAL_SDK_VERSION = importlib.metadata.version("octue")

_root_dir = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_PATH = os.path.join(_root_dir, "twined", "templates")
DATA_PATH = os.path.join(os.path.dirname(_root_dir), "tests", "data")
