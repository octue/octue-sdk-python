import json
import os
import tempfile
from unittest.mock import patch

import pkg_resources

from octue.resources import Datafile, Dataset, Manifest
from octue.utils.encoders import OctueJSONEncoder

from ..utils import ServicePatcher


try:
    from octue.cloud.emulators._pub_sub import MockService
except ModuleNotFoundError:
    try:
        from tests.cloud.pub_sub.mocks import MockService
    except ModuleNotFoundError:
        from octue.cloud.emulators.pub_sub import MockService

from octue.resources.service_backends import GCPPubSubBackend


path = tempfile.NamedTemporaryFile().name

input_manifest = Manifest(
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


class QuestionRecorder:
    def __init__(self):
        self.question = None

    def __call__(self, topic, data, retry, *args, **attributes):
        self.question = {"data": data.decode(), "attributes": attributes}


def record_question():
    backend = GCPPubSubBackend(project_name="octue-amy")
    child = MockService(backend=backend, run_function=lambda: None)
    child.answer = lambda *args, **kwargs: None
    parent = MockService(backend=backend, run_function=lambda: None, children={child.id: child})

    with ServicePatcher():
        publish_patch, question_recorder = _get_and_start_publish_patch()

        try:
            child.serve()

            try:
                parent.ask(
                    child.id,
                    input_values={"height": 4, "width": 72},
                    input_manifest=input_manifest,
                    allow_local_files=True,
                )

            except KeyError:
                pass
            except Exception:
                parent.ask(
                    child.id,
                    input_values={"height": 4, "width": 72},
                    input_manifest=input_manifest,
                )

        finally:
            publish_patch.stop()

    with open(
        "/Users/Marcus1/repos/octue/octue-sdk-python/scripts/record_questions_across_versions/recorded_questions.jsonl",
        "a",
    ) as f:
        f.write(
            json.dumps(
                {
                    "parent_sdk_version": pkg_resources.get_distribution("octue").version,
                    "question": question_recorder.question,
                },
                cls=OctueJSONEncoder,
            )
            + "\n"
        )


def _get_and_start_publish_patch():
    try:
        publish_patch = patch("octue.cloud.emulators._pub_sub.MockPublisher.publish", QuestionRecorder())
        return publish_patch, publish_patch.start()
    except (ModuleNotFoundError, AttributeError):
        try:
            publish_patch = patch("tests.cloud.pub_sub.mocks.MockPublisher.publish", QuestionRecorder())
            return publish_patch, publish_patch.start()
        except (ModuleNotFoundError, AttributeError):
            publish_patch = patch("octue.cloud.emulators.pub_sub.MockPublisher.publish", QuestionRecorder())
            return publish_patch, publish_patch.start()


if __name__ == "__main__":
    record_question()
