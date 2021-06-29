import json
import logging

from octue.definitions import OUTPUT_STRANDS
from octue.mixins import Hashable, Identifiable, Labelable, Loggable, Serialisable, Taggable
from octue.resources.manifest import Manifest
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.folders import get_file_name_from_strand
from twined import ALL_STRANDS, Twine


module_logger = logging.getLogger(__name__)


_HASH_FUNCTIONS = {
    "configuration_values": Hashable.hash_non_class_object,
    "configuration_manifest": lambda manifest: manifest.hash_value,
    "input_values": Hashable.hash_non_class_object,
    "input_manifest": lambda manifest: manifest.hash_value,
}

# Map strand names to class which we expect Twined to instantiate for us
CLASS_MAP = {"configuration_manifest": Manifest, "input_manifest": Manifest, "output_manifest": Manifest}


class Analysis(Identifiable, Loggable, Serialisable, Labelable, Taggable):
    """Analysis class, holding references to all input and output data

    ## The Analysis Instance

    An Analysis instance is unique to a specific computation analysis task, however large or small, run at a specific
    time. It will be created by the task runner (which will have validated incoming data already - Analysis() doesn't
    do any validation).

    It holds references to all config, input and output data, logs, connections to child twins, credentials, etc, so
    should be referred to from your code to get those items.

    It's basically the "Internal API" for your data service - a single point of contact where you can get or update
    anything you need.

    Analyses are instantiated at the top level of your app/service/twin code and you can import the instantiated
    object from there (see the templates for examples)

    :parameter twine: Twine instance or json source
    :parameter configuration_values: see Runner.run() for definition
    :parameter configuration_manifest: see Runner.run() for definition
    :parameter input_values: see Runner.run() for definition
    :parameter input_manifest: see Runner.run() for definition
    :parameter credentials: see Runner.run() for definition
    :parameter monitors: see Runner.run() for definition
    :parameter output_values: see Runner.run() for definition
    :parameter output_manifest: see Runner.run() for definition
    :parameter id: Optional UUID for the analysis
    :parameter logger: Optional logging.Logger instance attached to the analysis
    """

    def __init__(self, twine, skip_checks=False, **kwargs):
        """Constructor of Analysis instance"""

        # Instantiate the twine (if not already) and attach it to self
        if not isinstance(twine, Twine):
            twine = Twine(source=twine)

        self.twine = twine
        self._skip_checks = skip_checks

        # Pop any possible strand data sources before init superclasses (and tie them to protected attributes)
        strand_kwargs = [(name, kwargs.pop(name, None)) for name in ALL_STRANDS]
        for strand_name, strand_data in strand_kwargs:
            setattr(self, f"{strand_name}", strand_data)

        for strand_name, strand_data in strand_kwargs:
            if strand_name in _HASH_FUNCTIONS:
                strand_hash_name = f"{strand_name}_hash"

                if strand_data is not None:
                    setattr(self, strand_hash_name, _HASH_FUNCTIONS[strand_name](strand_data))
                else:
                    setattr(self, strand_hash_name, None)

        # Init superclasses
        super().__init__(**kwargs)

    def finalise(self, output_dir=None, save_locally=False, upload_to_cloud=False, project_name=None, bucket_name=None):
        """Validate and serialise the output values and manifest, optionally writing them to files and/or the manifest
        to the cloud.

        :param str output_dir: path-like pointing to directory where the outputs should be saved to file (if None, files
            are not written)
        :param bool save_locally:
        :param bool upload_to_cloud:
        :param str project_name:
        :param str bucket_name:
        :return dict: serialised strings for values and manifest data.
        """
        serialised_strands = {}

        for output_strand in OUTPUT_STRANDS:
            self.logger.debug("Serialising %r", output_strand)

            attribute = getattr(self, output_strand)
            if attribute is not None:
                attribute = json.dumps(attribute, cls=OctueJSONEncoder)

            serialised_strands[output_strand] = attribute

        self.logger.debug("Validating serialised output json against twine")
        self.twine.validate(**serialised_strands)

        # Optionally write the serialised strands to disk.
        if save_locally:
            for output_strand in OUTPUT_STRANDS:
                if serialised_strands[output_strand] is not None:

                    filename = get_file_name_from_strand(output_strand, output_dir)
                    with open(filename, "w") as fp:
                        fp.write(serialised_strands[output_strand])
                    self.logger.debug("Wrote %r to file %r", output_strand, filename)

        # Optionally write the manifest to Google Cloud storage.
        if upload_to_cloud:
            if hasattr(self, "output_manifest"):
                self.output_manifest.to_cloud(project_name, bucket_name, output_dir)
                self.logger.debug(
                    "Wrote %r to cloud storage at project %r in bucket %r.",
                    self.output_manifest,
                    project_name,
                    bucket_name,
                )

        return serialised_strands
