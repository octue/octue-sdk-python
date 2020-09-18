import logging
import os
import uuid

from octue.exceptions import FolderNotFoundException, InvalidInputException
from octue.resources.manifest import Manifest
from octue.utils import isfolder
from twined import Twine


module_logger = logging.getLogger(__name__)


FOLDERS = (
    "configuration",
    "input",
    "log",
    "tmp",
    "output",
)


class Runner:
    """ Runs analyses in the app framework

    The Runner class provides a set of configuration parameters for use by your application, together with a range of
    methods for managing input and output file parsing as well as controlling logging.

    :parameter twine: string path to the twine file, or a string containing valid twine json

    :parameter paths: string or dict. If a string, contains a single path to an existing data directory where
    (if not already present), subdirectories 'configuration', 'input', 'tmp', 'log' and 'output' will be created. If a
    dict, it should contain all of those keys, with each of their values being a path to a directory (which will be
    recursively created if it doesn't exist)

    """

    def __init__(self, twine="twine.json", paths="data"):

        # Set paths
        self.paths = dict()
        if isinstance(paths, str):
            if not os.path.isdir(paths):
                raise FolderNotFoundException(f"Specified data folder '{paths}' not present")

            self.paths = dict([(folder, os.path.join(paths, folder)) for folder in FOLDERS])

        else:
            if (
                not isinstance(paths, dict)
                or (len(paths.keys()) != len(FOLDERS))
                or not all([k in FOLDERS for k in paths.keys()])
            ):
                raise InvalidInputException(
                    f"Input 'paths' should be a dict containing directory paths with the following keys: {FOLDERS}"
                )

            self.paths = paths

        # Ensure paths exist on disc
        for folder in FOLDERS:
            isfolder(self.paths[folder], make_if_absent=True)

        # Ensure the twine is present and instantiate it
        self.twine = Twine(source=twine)

        # Initialise a run-specific logger, with unified formatting
        self._init_log()

        # Create configuration variables, which must be initialised with the configure() method before calling run()
        self.configuration_manifest = None
        self.configuration_values = None

        # Analyses. Multiple analysis objects can be created
        self.analyses = {}

    @staticmethod
    def get_run_logger(run_id, level=logging.INFO):
        """ Configures application level console logging options

        :parameter run_id: The id of the run to get the log for. Should be unique to the run
        :type run_id: str

        :parameter level: Log level for the run, see python's logging module for levels. Default: `logging.INFO`
        :type level: int

        :return: logger
        """

        # TODO add file handlers and octue-specific and app-specific loggers.
        #  See https://docs.python.org/3/howto/logging.html#logging-basic-tutorial  to understand the logger hierarchy
        # TODO allow varied log level
        logging.basicConfig(
            format="%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s", level=logging.DEBUG
        )

        run_logger = logging.getLogger(f"run-{run_id}")

        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        console_handler.setFormatter(formatter)

        run_logger.addHandler(console_handler)

    @property
    def logger(self):
        """ Returns either the custom logger for this run, or the default module logger (for runner.py)
        """
        return self._logger or module_logger

    def configure(self, values=None, manifest=None, skip_file_checks=False):
        """ Configure the runner, using configuration values and manifest (from file or passed in)
        :parameter values: The configuration_values data. Can be expressed as a string path of a *.json file (relative
        or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :parameter manifest: The configuration_manifest data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :parameter skip_file_checks: If true, skip the check that all files in the manifest are present on disc - this
        can be an extremely long process for large datasets.
        :return: None
        """
        if not values:
            self.logger.info("No configuration values passed. Using {} as default")
            values = {}

        self.configuration_values = self.twine.validate_configuration_values(values)

        if not manifest:
            self.logger.info("No configuration manifest passed. Using empty manifest as default")
            manifest = {
                "id": str(uuid.uuid4()),
                "datasets": [],
            }

        self.configuration_manifest = self.twine.validate_configuration_manifest(manifest, manifest_class=Manifest)

    def run(self, values=None, manifest=None, skip_file_checks=False):
        """ Run an analysis
        :parameter values: The input_values data. Can be expressed as a string path of a *.json file (relative
        or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :parameter manifest: The input_manifest data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :parameter skip_file_checks: If true, skip the check that all files in the manifest are present on disc - this
        can be an extremely long process for large datasets.
        :return: None
        """

        # TODO collapse into a common method with the contents of configure
        if not values:
            self.logger.info("No input values passed. Using {} as default")
            values = {}

        input_values = self.twine.validate_input_values(values)

        if not manifest:
            self.logger.info("No input manifest passed. Using empty manifest as default")
            manifest = {
                "id": str(uuid.uuid4()),
                "datasets": [],
            }

        input_manifest = self.twine.validate_input_manifest(manifest, manifest_class=Manifest)

        self.logger.info("DO SOMETHING WITH INPUT VALUES", input_values)
        self.logger.info("DO SOMETHING WITH INPUT Manifest", input_manifest)
        output_values = {}
        output_manifest = Manifest()

        output_manifest = self.twine.validate_output_manifest(output_manifest)

        return output_values, output_manifest
