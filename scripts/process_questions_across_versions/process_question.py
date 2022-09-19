import base64
import json
import os
import sys
import tempfile
from unittest.mock import patch

from octue.resources import Datafile, Dataset, Manifest


try:
    from octue.cloud.emulators._pub_sub import MESSAGES, MockService, MockSubscriber, MockSubscription, MockTopic
except ModuleNotFoundError:
    try:
        from tests.cloud.pub_sub.mocks import MESSAGES, MockService, MockSubscriber, MockSubscription, MockTopic
    except ModuleNotFoundError:
        from octue.cloud.emulators.pub_sub import MockService, MockSubscriber, MockSubscription, MockTopic, MESSAGES

from octue.resources.service_backends import GCPPubSubBackend


path = tempfile.NamedTemporaryFile().name

output_manifest = Manifest(
    datasets={
        "my_dataset": Dataset(
            path=path,
            files=[
                Datafile(path=os.path.join(path, "path-within-dataset", "a_test_file.csv")),
                Datafile(path=os.path.join(path, "path-within-dataset", "another_test_file.csv")),
            ],
        )
    }
)


class ServicePatcher:
    def __init__(self, patches=None):
        self.patches = patches or [
            patch("octue.cloud.pub_sub.service.Topic", new=MockTopic),
            patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription),
            patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber),
        ]

    def __enter__(self):
        """Start the patches and return the mocks they produce.

        :return list(unittest.mock.MagicMock):
        """
        return [patch.start() for patch in self.patches]

    def __exit__(self, *args, **kwargs):
        """Stop the patches.

        :return None:
        """
        for p in self.patches:
            p.stop()


class MockAnalysis:
    """A mock Analysis object with just the output strands.

    :param any output_values:
    :param octue.resources.manifest.Manifest|None output_manifest:
    :return None:
    """

    def __init__(self, output_values="Hello! It worked!", output_manifest=None):
        self.output_values = output_values
        self.output_manifest = output_manifest


with open(sys.argv[1]) as f:
    question = json.load(f)

print(f"Processing question from version {question['parent_sdk_version']}... ", end="", flush=False)
question["question"]["data"] = base64.b64encode(question["question"]["data"].encode())

backend = GCPPubSubBackend(project_name="octue-amy")
child = MockService(backend=backend, run_function=lambda: None)
MESSAGES[child.id + ".answers." + question["question"]["attributes"]["question_uuid"]] = []

try:
    # Check serialised input manifests can be parsed.
    deserialised_question_data = json.loads(base64.b64decode(question["question"]["data"]).decode())
    Manifest.deserialise(json.loads(deserialised_question_data["input_manifest"]))

    # Check the rest of the question can be parsed.
    with ServicePatcher():
        child.serve()
        child.run_function = lambda *args, **kwargs: MockAnalysis(
            output_values=[1, 2, 3, 4],
            output_manifest=output_manifest,
        )
        child.answer(question["question"])

    print("succeeded.")

except Exception as error:
    print("failed.")
    raise error
