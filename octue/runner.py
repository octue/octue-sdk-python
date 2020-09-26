import logging

from octue.resources.analysis import CLASS_MAP, Analysis
from octue.utils import gen_uuid
from twined import Twine


module_logger = logging.getLogger(__name__)

# Logging format for analysis runs. All handlers should use this logging format, to make logs consistently parseable
LOG_FORMAT = "%(name)s %(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s"


class Runner:
    """ Runs analyses in the app framework

    The Runner class provides a set of configuration parameters for use by your application, together with a range of
    methods for managing input and output file parsing as well as controlling logging.

    :parameter twine: string path to the twine file, or a string containing valid twine json

    :parameter paths: string or dict. If a string, contains a single path to an existing data directory where
    (if not already present), subdirectories 'configuration', 'input', 'tmp', 'log' and 'output' will be created. If a
    dict, it should contain all of those keys, with each of their values being a path to a directory (which will be
    recursively created if it doesn't exist)

    :parameter configuration_values: The strand data. Can be expressed as a string path of a *.json file (relative
    or absolute), as an open file-like object (containing json data), as a string of json data or as an
    already-parsed dict.

    :parameter configuration_manifest: The strand data. Can be expressed as a string path of a *.json file
    (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
    already-parsed dict.

    :parameter skip_file_checks: If true, skip the check that all files in the manifest are present on disc - this
    can be an extremely long process for large datasets.
    """

    def __init__(
        self, twine="twine.json", configuration_values=None, configuration_manifest=None, log_level=logging.INFO
    ):
        """ Constructor for the Runner class
        """

        # Ensure the twine is present and instantiate it
        self.twine = Twine(source=twine)

        # Validate and initialise configuration data
        self.configuration_values = self.twine.validate(configuration_values=configuration_values)
        self.configuration_manifest = self.twine.validate(
            configuration_manifest=configuration_manifest, cls=CLASS_MAP["configuration_manifest"]
        )

        # Store the log level (same log level used for all analyses)
        self._log_level = log_level

        # Store analyses. Multiple analysis objects can be created and coexist.
        self.analyses = {}

    def _get_default_handler(self):
        """ Gets a basic console handler set up for logging analyses
        """
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self._log_level)
        formatter = logging.Formatter(LOG_FORMAT)
        console_handler.setFormatter(formatter)

        return console_handler

    def _get_analysis_logger(self, analysis_id, handler=None):
        """ Create a logger specific to the analysis

        :parameter analysis_id: The id of the analysis to get the log for. Should be unique to the analysis
        :type analysis_id: str

        :parameter handler: The handler to use. If None, default console handler will be attached.

        :return: logger named in the pattern `analysis-{analysis_id}`
        :rtype logging.Logger
        """

        handler = handler or self._get_default_handler()
        analysis_logger = logging.getLogger(f"analysis-{analysis_id}")
        analysis_logger.addHandler(handler)

        return analysis_logger

    def run(self, fcn, handler=None, input_values=None, input_manifest=None, credentials=None, children=None):
        """ Run an analysis

        :parameter input_values: The input_values strand data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :type input_values (str, dict)

        :parameter input_manifest: The input_manifest strand data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :type input_manifest (str, Manifest)

        :parameter credentials: The credentials strand data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :type credentials (str, dict)

        :parameter children: The children strand data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :type children: (str, dict)

        :parameter handler: the logging.Handler instance which will be used to handle logs for this analysis run.
        handlers can be created as per the logging cookbook https://docs.python.org/3/howto/logging-cookbook.html but
        should use the format defined above in LOG_FORMAT.
        :type handler: logging.Handler

        :return: None
        """

        inputs = self.twine.validate(
            input_values=input_values,
            input_manifest=input_manifest,
            credentials=credentials,
            children=children,
            cls=CLASS_MAP,
            allow_missing=False,
            allow_extra=False,
        )

        outputs_and_monitors = self.twine.prepare("monitors", "output_values", "output_manifest", cls=CLASS_MAP)

        analysis_id = gen_uuid()
        analysis_logger = self._get_analysis_logger(analysis_id, handler)
        analysis = Analysis(
            id=analysis_id,
            logger=analysis_logger,
            twine=self.twine,
            configuration_values=self.configuration_values,
            configuration_manifest=self.configuration_manifest,
            **inputs,
            **outputs_and_monitors,
        )

        try:
            fcn(analysis)
            self.twine.validate(output_values=analysis.output_values)
            self.twine.validate(output_manifest=analysis.output_manifest)

        except Exception as e:
            analysis_logger.error(str(e))
            raise e

        return analysis
