import json
import os

from octue.resources import Manifest
from octue.utils.decoders import OctueJSONDecoder


def load_test_fixture(path, serialise=True):
    """Load a test fixture from the given path to a local directory created by the `octue get-crash-diagnostics` CLI
    command. The configuration values, configuration manifest, input values, and input manifest are returned if
    available.

    :param str path: the path to a local directory containing downloaded crash diagnostics data
    :param bool serialise: if `True`, return the values and manifests in their serialised form.
    :return (any|str|None, octue.resources.manifest.Manifest|str|None, any|str|None, octue.resources.manifest.Manifest|str|None): the configuration values, configuration manifest, input values, and input manifest (each if available)
    """
    paths = {
        name: os.path.join(path, name + ".json")
        for name in ("configuration_values", "configuration_manifest", "input_values", "input_manifest")
    }

    outputs = {"values": {}, "manifest": {}}

    for name, path in paths.items():
        if not os.path.exists(path):
            continue

        stage, data_type = name.split("_")

        with open(path) as f:
            outputs[data_type][stage] = f.read()

    if not serialise:
        for name, data in outputs["values"].items():
            outputs["values"][name] = json.loads(data, cls=OctueJSONDecoder)

        for name, manifest in outputs["manifest"].items():
            outputs["manifest"][name] = Manifest.deserialise(manifest, from_string=True)

    return (
        outputs["values"].get("configuration"),
        outputs["manifest"].get("configuration"),
        outputs["values"].get("input"),
        outputs["manifest"].get("input"),
    )
