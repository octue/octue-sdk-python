import os


TESTS_DIR = os.path.dirname(__file__)
TEST_PROJECT_ID = "octue-sdk-python-test-project"
TEST_BUCKET_NAME = "octue-sdk-python-test-bucket"
MOCK_SERVICE_REVISION_TAG = "2.3.0"


os.environ["USE_OCTUE_LOG_HANDLER"] = "1"
