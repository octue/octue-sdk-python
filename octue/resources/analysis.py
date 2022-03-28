import json
import logging
import warnings

import twined.exceptions
from octue.cloud import storage
from octue.definitions import OUTPUT_STRANDS
from octue.exceptions import InvalidMonitorMessage
from octue.mixins import Hashable, Identifiable, Labelable, Serialisable, Taggable
from octue.resources.manifest import Manifest
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.folders import get_file_name_from_strand
from twined import ALL_STRANDS, Twine


logger = logging.getLogger(__name__)


HASH_FUNCTIONS = {
    "configuration_values": Hashable.hash_non_class_object,
    "configuration_manifest": lambda manifest: manifest.hash_value,
    "input_values": Hashable.hash_non_class_object,
    "input_manifest": lambda manifest: manifest.hash_value,
}

# Map strand names to class which we expect Twined to instantiate for us
CLASS_MAP = {"configuration_manifest": Manifest, "input_manifest": Manifest, "output_manifest": Manifest}


class Analysis(Identifiable, Serialisable, Labelable, Taggable):
    """A class representing a scientific or computational analysis, holding references to all configuration, input, and
    output data.

    An Analysis instance is unique to a specific computational analysis task, however large or small, run at a specific
    time. It will be created by the task runner, which will have validated incoming data already (Analysis doesn't
    do any validation).

    It holds references to all configuration, input, and output data, logs, connections to child twins, credentials,
    etc, so should be referred to from your code to get those items.

    It's basically the "Internal API" for your data service - a single point of contact where you can get or update
    anything you need.

    Analyses are instantiated at the top level of your app/service/twin code and you can import the instantiated
    object from there (see the templates for examples)

    :param twined.Twine|str twine: Twine instance or json source
    :param callable|None handle_monitor_message: a function that sends monitor messages to the parent that requested the analysis
    :param any configuration_values: see Runner.run() for definition
    :param octue.resources.manifest.Manifest configuration_manifest: see Runner.run() for definition
    :param any input_values: see Runner.run() for definition
    :param octue.resources.manifest.Manifest input_manifest: see Runner.run() for definition
    :param dict credentials: see Runner.run() for definition
    :param dict monitors: see Runner.run() for definition
    :param any output_values: see Runner.run() for definition
    :param octue.resources.manifest.Manifest output_manifest: see Runner.run() for definition
    :param str id: Optional UUID for the analysis
    :return None:
    """

    def __init__(self, twine, handle_monitor_message=None, skip_checks=False, **kwargs):
        if isinstance(twine, Twine):
            self.twine = twine
        else:
            self.twine = Twine(source=twine)

        self._handle_monitor_message = handle_monitor_message

        strand_kwargs = {name: kwargs.pop(name, None) for name in ALL_STRANDS}

        # Values strands.
        self.input_values = strand_kwargs.get("input_values", None)
        self.configuration_values = strand_kwargs.get("configuration_values", None)
        self.output_values = strand_kwargs.get("output_values", None)

        # Manifest strands.
        self.configuration_manifest = strand_kwargs.get("configuration_manifest", None)
        self.input_manifest = strand_kwargs.get("input_manifest", None)
        self.output_manifest = strand_kwargs.get("output_manifest", None)

        # Other strands.
        self.credentials = strand_kwargs.get("credentials", None)
        self.children = strand_kwargs.get("children", None)
        self.monitors = strand_kwargs.get("monitors", None)

        self._calculate_strand_hashes(strands=strand_kwargs)
        self._skip_checks = skip_checks

        super().__init__(**kwargs)

    def send_monitor_message(self, data):
        """Send a monitor message to the parent that requested the analysis.

        :param any data: any JSON-compatible data structure
        :return None:
        """
        try:
            self.twine.validate_monitor_message(source=data)
        except twined.exceptions.InvalidValuesContents as e:
            raise InvalidMonitorMessage(e)

        if self._handle_monitor_message is None:
            logger.warning("Attempted to send a monitor message but no handler is specified.")
            return

        self._handle_monitor_message(data)

    def finalise(self, output_dir=None, save_locally=False, upload_to_cloud=False, cloud_path=None, bucket_name=None):
        """Validate and serialise the output values and manifest, optionally writing them to files and/or the manifest
        to the cloud.

        :param str output_dir: path-like pointing to directory where the outputs should be saved to file (if None, files are not written)
        :param bool save_locally:
        :param bool upload_to_cloud:
        :param str bucket_name:
        :return dict: serialised strings for values and manifest data.
        """
        serialised_strands = {"output_values": None, "output_manifest": None}

        if self.output_values:
            serialised_strands["output_values"] = json.dumps(self.output_values, cls=OctueJSONEncoder)

        if self.output_manifest:
            serialised_strands["output_manifest"] = self.output_manifest.to_primitive()

        logger.debug("Validating serialised output json against twine")
        self.twine.validate(**serialised_strands)

        # Optionally write the serialised strands to disk.
        if save_locally:
            for output_strand in OUTPUT_STRANDS:
                if serialised_strands[output_strand] is not None:

                    filename = get_file_name_from_strand(output_strand, output_dir)
                    with open(filename, "w") as fp:
                        fp.write(serialised_strands[output_strand])
                    logger.debug("Wrote %r to file %r", output_strand, filename)

        # Optionally write the manifest to Google Cloud storage.
        if upload_to_cloud:
            if hasattr(self, "output_manifest"):
                if not cloud_path:
                    warnings.warn(
                        message=(
                            "Using a bucket name and path in bucket will be deprecated soon. Please use `cloud_path` instead e.g."
                            "'gs://bucket_name/path/to/file.txt'."
                        ),
                        category=DeprecationWarning,
                    )

                    cloud_path = storage.path.generate_gs_path(bucket_name, output_dir)

                self.output_manifest.to_cloud(cloud_path)
                logger.debug("Wrote %r to %r.", self.output_manifest, cloud_path)

        return serialised_strands

    def _calculate_strand_hashes(self, strands):
        """Calculate the hashes of the strands specified in the HASH_FUNCTIONS constant.

        :param dict strands: strand names mapped to strand data
        :return None:
        """
        for strand_name, strand_data in strands.items():
            if strand_name in HASH_FUNCTIONS:
                strand_hash_name = f"{strand_name}_hash"

                if strand_data is not None:
                    setattr(self, strand_hash_name, HASH_FUNCTIONS[strand_name](strand_data))
                else:
                    setattr(self, strand_hash_name, None)
