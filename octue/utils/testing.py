import json
import os

from octue.cloud.emulators.child import ChildEmulator
from octue.resources import Manifest


def load_test_fixture(path):
    """Load a test fixture from the given path to a local directory created by the `octue get-crash-diagnostics` CLI
    command. The configuration values, configuration manifest, input values, and input manifest are returned if
    available. A list of child emulators is returned if the service has children and asked questions to any of them.
    Each child emulator corresponds to one question asked to a child. The child emulators are in the same order as the
    questions were asked by the service during its analysis.

    :param str path: the path to a local directory containing downloaded crash diagnostics data
    :return (any|str|None, octue.resources.manifest.Manifest|str|None, any|str|None, octue.resources.manifest.Manifest|str|None, list(octue.cloud.emulators.child.ChildEmulator)): the configuration values, configuration manifest, input values, input manifest (each if available), and a list of child emulators corresponding to the questions asked by the service
    """
    return (
        _load_values(path, "configuration"),
        _load_manifest(path, "configuration"),
        _load_values(path, "input"),
        _load_manifest(path, "input"),
        _load_child_emulators(path),
    )


def _load_values(path, stage):
    path = os.path.join(path, f"{stage}_values.json")

    if not os.path.exists(path):
        return

    with open(path) as f:
        return json.load(f)


def _load_manifest(path, stage):
    path = os.path.join(path, f"{stage}_manifest.json")

    if os.path.exists(path):
        return Manifest.from_file(path)


def _load_child_emulators(path):
    with open(os.path.join(path, "questions.json")) as f:
        questions = json.load(f)

    return tuple(ChildEmulator(id=question["id"], messages=question["messages"]) for question in questions)
