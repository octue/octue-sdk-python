import importlib.metadata

VALUES_FILENAME = "values.json"
MANIFEST_FILENAME = "manifest.json"

OUTPUT_STRANDS = ("output_values", "output_manifest")
RUN_STRANDS = ("input_values", "input_manifest", "credentials", "children")

GOOGLE_COMPUTE_PROVIDERS = {"GOOGLE_CLOUD_FUNCTION"}
LOCAL_SDK_VERSION = importlib.metadata.version("octue")
DEFAULT_MAXIMUM_HEARTBEAT_INTERVAL = 360
