import json
import os

from octue.cloud.emulators.child import ChildEmulator
from octue.definitions import MANIFEST_FILENAME, VALUES_FILENAME
from octue.resources import Manifest


def load_test_fixture_from_diagnostics(path):
    """Load a test fixture from service diagnostics downloaded using the `octue get-diagnostics` CLI
    command. The configuration values, configuration manifest, input values, and input manifest are returned if
    available. A tuple of child emulators is returned if the service has children and asked questions to any of them.
    Each child emulator corresponds to one question asked to a child. The child emulators are in the same order as the
    questions were asked by the service during its analysis.

    :param str path: the path to a local directory containing downloaded diagnostics data
    :return (any|None, octue.resources.manifest.Manifest|None, any|None, octue.resources.manifest.Manifest|None, tuple(octue.cloud.emulators.child.ChildEmulator)): the configuration values, configuration manifest, input values, input manifest (each if available), and a tuple of child emulators corresponding to the questions asked by the service
    """
    return (
        _load_values(path, "configuration"),
        _load_manifest(path, "configuration"),
        _load_values(path, "input"),
        _load_manifest(path, "input"),
        _load_child_emulators(path),
    )


def _load_values(path, stage):
    """Load values from a JSON file in the directory specified by `path`.

    :param str path: the path to the diagnostics directory
    :param str stage: one of "configuration" or "input"
    :return any|None: the values or `None` if no relevant file exists in the directory specified by `path`
    """
    path = os.path.join(path, f"{stage}_{VALUES_FILENAME}")

    if not os.path.exists(path):
        return

    with open(path) as f:
        return json.load(f)


def _load_manifest(path, stage):
    """Load a manifest from a JSON file in the directory specified by `path`.

    :param str path: the path to the diagnostics directory
    :param str stage: one of "configuration" or "input"
    :return any|None: the manifest or `None` if no relevant file exists in the directory specified by `path`
    """
    path = os.path.join(path, f"{stage}_{MANIFEST_FILENAME}")

    if not os.path.exists(path):
        return

    return Manifest.from_file(path)


def _load_child_emulators(path):
    """Load child emulators from a JSON file in the directory specified by `path`.

    :param str path: the path to the diagnostics directory
    :return tuple(octue.resources.child.Child):
    """
    with open(os.path.join(path, "questions.json")) as f:
        questions = json.load(f)

    return tuple(ChildEmulator(id=question["id"], messages=question["messages"]) for question in questions)
