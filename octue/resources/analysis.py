import json
import logging

import coolname

import twined.exceptions
from octue.cloud import storage
from octue.exceptions import InvalidMonitorMessage
from octue.mixins import Hashable, Identifiable, Labelable, Serialisable, Taggable
from octue.resources.manifest import Manifest
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.threads import RepeatingTimer
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
    """A class representing a scientific or computational analysis. It holds references to all configuration, input, and
    output data, logs, connections to child services, credentials, etc. It's essentially the "Internal API" for your
    service - a single point of contact where you can get or update anything you need.

    An ``Analysis`` instance is automatically provided to the app in an Octue service when a question is received. Its
    attributes include every strand that can be added to a ``Twine``, although only the strands specified in the
    service's twine will be non-``None``. Incoming data is validated before it's added to the analysis.

    All input and configuration attributes are hashed using a `BLAKE3 hash <https://github.com/BLAKE3-team/BLAKE3>`_ so
    the inputs and configuration that produced a given output in your app can always be verified. These hashes exist on
    the following attributes:

    -   ``input_values_hash``
    -   ``input_manifest_hash``
    -   ``configuration_values_hash``
    -   ``configuration_manifest_hash``

    If a strand is ``None``, so will its corresponding hash attribute be. The hash of a datafile is the hash of its
    file, while the hash of a manifest or dataset is the cumulative hash of the files it refers to.

    :param twined.Twine|dict|str twine: the twine, dictionary defining a twine, or path to "twine.json" file defining the service's data interface
    :param callable|None handle_monitor_message: an optional function for sending monitor messages to the parent that requested the analysis
    :param any configuration_values: the configuration values for the analysis - this can be expressed as a python primitive (e.g. dict), a path to a JSON file, or a JSON string.
    :param octue.resources.manifest.Manifest configuration_manifest: a manifest of configuration datasets for the analysis if required
    :param any input_values: the input values for the analysis - this can be expressed as a python primitive (e.g. dict), a path to a JSON file, or a JSON string.
    :param octue.resources.manifest.Manifest input_manifest: a manifest of input datasets for the analysis if required
    :param any output_values: any output values the analysis produces
    :param octue.resources.manifest.Manifest output_manifest: a manifest of output dataset from the analysis if it produces any
    :param dict children: a mapping of string key to ``Child`` instance for all the children used by the service
    :param str id: Optional UUID for the analysis
    :return None:
    """

    def __init__(self, twine, handle_monitor_message=None, **kwargs):
        if isinstance(twine, Twine):
            self.twine = twine
        else:
            self.twine = Twine(source=twine)

        self._handle_monitor_message = handle_monitor_message

        strand_kwargs = {name: kwargs.pop(name, None) for name in ALL_STRANDS}

        # Values strands.
        self.configuration_values = strand_kwargs.get("configuration_values", None)
        self.input_values = strand_kwargs.get("input_values", None)
        self.output_values = strand_kwargs.get("output_values", None)

        # Manifest strands.
        self.configuration_manifest = strand_kwargs.get("configuration_manifest", None)
        self.input_manifest = strand_kwargs.get("input_manifest", None)
        self.output_manifest = strand_kwargs.get("output_manifest", None)

        # Other strands.
        self.children = strand_kwargs.get("children", None)

        # Non-strands.
        self.output_location = kwargs.pop("output_location", None)
        self.use_signed_urls_for_output_datasets = kwargs.pop("use_signed_urls_for_output_datasets", False)

        self._calculate_strand_hashes(strands=strand_kwargs)
        self._periodic_monitor_message_sender_threads = []
        self._finalised = False
        super().__init__(**kwargs)

    @property
    def finalised(self):
        """Check whether the analysis has been finalised (i.e. whether its outputs have been validated and, if an output
        manifest is produced, its datasets uploaded).

        :return bool:
        """
        return self._finalised

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

    def set_up_periodic_monitor_message(self, create_monitor_message, period=60):
        """Set up a periodic monitor message that sends up-to-date data once per period.

        :param callable create_monitor_message: a callable that takes no arguments and returns a new up-to-date monitor message to send each time it's called
        :param int|float period: the repetition period in seconds
        :return None:
        """
        thread = RepeatingTimer(
            interval=period,
            function=lambda: self.send_monitor_message(data=create_monitor_message()),
        )

        thread.daemon = True
        thread.start()

        self._periodic_monitor_message_sender_threads.append(thread)
        logger.info("Periodic monitor message set up to send every %ss.", period)

    def finalise(self, upload_output_datasets_to=None, use_signed_urls=None):
        """Validate the output values and output manifest and, if the analysis produced an output manifest, upload its
        output datasets to a unique subdirectory within the analysis's output location. This output location can be
        overridden by providing a different cloud path via the `upload_output_datasets_to` parameter.

        :param str|None upload_output_datasets_to: If not provided but an output location was provided at instantiation, upload any output datasets into a unique subdirectory within this output location; if provided, upload into this location instead. The output manifest is updated with the upload locations.
        :param bool|None use_signed_urls: if `True`, use signed URLs instead of cloud URIs for dataset paths in the output manifest; if `None`, use the value of `use_signed_urls_for_output_datasets` given at instantiation
        :return None:
        """
        serialised_strands = {"output_values": None, "output_manifest": None}

        if self.output_values:
            logger.info("The analysis produced output values.")
            serialised_strands["output_values"] = json.dumps(self.output_values, cls=OctueJSONEncoder)
        else:
            logger.info("The analysis didn't produce output values.")

        if self.output_manifest:
            logger.info("The analysis produced an output manifest.")

            if not (self.output_location or upload_output_datasets_to):
                logger.info("No output location was set in the app configuration - can't upload output datasets.")

            serialised_strands["output_manifest"] = self.output_manifest.serialise()

        else:
            logger.info("The analysis didn't produce an output manifest.")

        self.twine.validate(**serialised_strands)
        self._finalised = True
        logger.info("Validated outputs against the twine.")

        if self.output_manifest and (self.output_location or upload_output_datasets_to):
            self._upload_output_datasets(upload_output_datasets_to, use_signed_urls)

    def _upload_output_datasets(self, upload_output_datasets_to, use_signed_urls):
        """Upload the output manifest's datasets.

        :param str|None upload_output_datasets_to: If not provided but an output location was provided at instantiation, upload any output datasets into a unique subdirectory within this output location; if provided, upload into this location instead. The output manifest is updated with the upload locations.
        :param bool|None use_signed_urls: if `True`, use signed URLs instead of cloud URIs for dataset paths in the output manifest; if `None`, use the value of `use_signed_urls_for_output_datasets` given at instantiation
        :return None:
        """
        # Use a unique subdirectory in the output location given at instantiation (if given) if no
        # `upload_output_datasets_to` is provided.
        if not upload_output_datasets_to:
            upload_output_datasets_to = storage.path.join(self.output_location, coolname.generate_slug())

        logger.info("Beginning upload of output datasets to %r...", upload_output_datasets_to)

        for name, dataset in self.output_manifest.datasets.items():
            dataset.upload(cloud_path=storage.path.join(upload_output_datasets_to, name))

        if use_signed_urls is None:
            use_signed_urls = self.use_signed_urls_for_output_datasets

        if use_signed_urls:
            self.output_manifest.use_signed_urls_for_datasets()

        logger.info("Finished uploading output datasets to %r.", upload_output_datasets_to)

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
