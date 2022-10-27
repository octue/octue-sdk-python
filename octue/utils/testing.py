import json
import os

from octue.cloud.emulators.child import ChildEmulator
from octue.resources import Manifest
from octue.utils.decoders import OctueJSONDecoder


def load_test_fixture(path, serialise=True):
    """Load a test fixture from the given path to a local directory created by the `octue get-crash-diagnostics` CLI
    command. The configuration values, configuration manifest, input values, and input manifest are returned if
    available. A list of child emulators is returned if the service has children and asked questions to any of them.
    Each child emulator corresponds to one question asked to a child. The child emulators are in the same order as the
    questions were asked by the service during its analysis.

    :param str path: the path to a local directory containing downloaded crash diagnostics data
    :param bool serialise: if `True`, return the values and manifests in their serialised form.
    :return (any|str|None, octue.resources.manifest.Manifest|str|None, any|str|None, octue.resources.manifest.Manifest|str|None, list(octue.cloud.emulators.child.ChildEmulator)): the configuration values, configuration manifest, input values, input manifest (each if available), and a list of child emulators corresponding to the questions asked by the service
    """
    paths = {
        name: os.path.join(path, name + ".json")
        for name in ("configuration_values", "configuration_manifest", "input_values", "input_manifest")
    }

    outputs = {"values": {}, "manifest": {}}

    # Load configuration and input data.
    for name, data_path in paths.items():
        if not os.path.exists(data_path):
            continue

        stage, data_type = name.split("_")

        with open(data_path) as f:
            outputs[data_type][stage] = f.read()

    # Deserialise configuration and input data if required.
    if not serialise:
        for name, data in outputs["values"].items():
            outputs["values"][name] = json.loads(data, cls=OctueJSONDecoder)

        for name, manifest in outputs["manifest"].items():
            outputs["manifest"][name] = Manifest.deserialise(manifest, from_string=True)

    # Create an emulated child for each question asked by the service.
    with open(os.path.join(path, "questions.json")) as f:
        questions = json.load(f)

    child_emulators = [ChildEmulator(id=question["id"], messages=question["messages"]) for question in questions]

    return (
        outputs["values"].get("configuration"),
        outputs["manifest"].get("configuration"),
        outputs["values"].get("input"),
        outputs["manifest"].get("input"),
        child_emulators,
    )
