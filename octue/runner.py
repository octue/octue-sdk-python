import importlib
import logging
import os
import sys

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
        """ Constructor for the Runner class. """

        # Ensure the twine is present and instantiate it
        self.twine = Twine(source=twine)

        if "configuration_values" not in self.twine.available_strands:
            configuration_values = None

        if "configuration_manifest" not in self.twine.available_strands:
            configuration_manifest = None

        # Validate and initialise configuration data
        self.configuration = self.twine.validate(
            configuration_values=configuration_values, configuration_manifest=configuration_manifest, cls=CLASS_MAP,
        )

        # Set path for configuration manifest.
        # TODO this is hacky, we need to rearchitect the twined validation so we can do this kind of thing in there
        self.configuration["configuration_manifest"] = self._update_manifest_path(
            self.configuration.get("configuration_manifest", None), configuration_manifest,
        )

        # Store the log level (same log level used for all analyses)
        self._log_level = log_level

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

    @staticmethod
    def _update_manifest_path(manifest, pathname):
        """ A Quick hack to stitch the new Pathable functionality in the 0.1.4 release into the CLI and runner.

        The way we define a manifest path can be more robustly implemented as we migrate functionality into the twined
        library

        :param manifest:
        :type manifest:
        :param pathname:
        :type pathname:
        :return:
        :rtype:
        """
        if manifest is not None and hasattr(pathname, "endswith"):
            if pathname.endswith(".json"):
                manifest.path = os.path.split(pathname)[0]

            # Otherwise do nothing and rely on manifest having its path variable set already
            return manifest

    def run(
        self,
        app_src,
        handler=None,
        input_values=None,
        input_manifest=None,
        credentials=None,
        children=None,
        output_manifest_path=None,
    ):
        """ Run an analysis

        :parameter app_src: Either: an instance of the AppFrom manager class which has a run() method, or
        a function which accepts a single parameter (the instantiated analysis), or a string pointing
        to an application folder (which should contain an 'app.py' function like the templates).
        :type app_src: Union[AppFrom, function, str]

        :parameter input_values: The input_values strand data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :type input_values: Union[str, dict]

        :parameter input_manifest: The input_manifest strand data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :type input_manifest: Union[str, Manifest]

        :parameter credentials: The credentials strand data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :type credentials: Union[str, dict]

        :parameter children: The children strand data. Can be expressed as a string path of a *.json file
        (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an
        already-parsed dict.
        :type children: Union[str, dict]

        :parameter output_manifest_path: Path where output data will be written
        :type output_manifest_path: Union[str, path-like]

        :parameter handler: the logging.Handler instance which will be used to handle logs for this analysis run.
        handlers can be created as per the logging cookbook https://docs.python.org/3/howto/logging-cookbook.html but
        should use the format defined above in LOG_FORMAT.
        :type handler: logging.Handler

        :return: None
        """
        if "input_manifest" not in self.twine.available_strands:
            input_manifest = None

        inputs = self.twine.validate(
            input_values=input_values,
            input_manifest=input_manifest,
            credentials=credentials,
            children=children,
            cls=CLASS_MAP,
            allow_missing=False,
            allow_extra=False,
        )

        # TODO this is hacky, we need to rearchitect the twined validation so we can do this kind of thing in there
        inputs["input_manifest"] = self._update_manifest_path(inputs.get("input_manifest", None), input_manifest,)

        outputs_and_monitors = self.twine.prepare("monitors", "output_values", "output_manifest", cls=CLASS_MAP)

        # TODO this is hacky, we need to rearchitect the twined validation so we can do this kind of thing in there
        outputs_and_monitors["output_manifest"] = self._update_manifest_path(
            outputs_and_monitors.get("output_manifest", None), output_manifest_path,
        )

        analysis_id = gen_uuid()
        analysis_logger = self._get_analysis_logger(analysis_id, handler)
        analysis = Analysis(
            id=analysis_id,
            logger=analysis_logger,
            twine=self.twine,
            **self.configuration,
            **inputs,
            **outputs_and_monitors,
        )

        try:
            if hasattr(app_src, "run"):
                app_src.run(analysis)
            elif isinstance(app_src, str):
                with AppFrom(app_src) as app:
                    app.run(analysis)
            else:
                app_src(analysis)

        except Exception as e:
            analysis_logger.error(str(e))
            raise e

        return analysis


def unwrap(fcn):
    """ Recurse through wrapping to get the raw function without decorators.
    """
    if hasattr(fcn, "__wrapped__"):
        return unwrap(fcn.__wrapped__)
    return fcn


class AppFrom:
    """ Context manager that imports module 'app' from user's code base at a location app_path.

     The manager will issue a warning if an existing module called "app" is already loaded.

     The manager makes a temporary addition to the system path (to ensure app is loaded from the correct path)

     The manager will unload the module (by deleting it from sys.modules) on exit, enabling

    with AppFrom('/path/to/dir') as app:
        Runner().run(app)

    """

    def __init__(self, app_path="."):
        self.app_path = os.path.abspath(os.path.normpath(app_path))
        module_logger.debug(f"Initialising AppFrom context at app_path {self.app_path}")
        self.app_module = None

    def __enter__(self):
        # Warn on an app present on the system path
        if "app" in sys.modules.keys():
            module_logger.warning(
                "Module 'app' already on system path. Using 'AppFrom' context will yield unexpected results. Avoid using 'app' as a python module, except for your main entrypoint"
            )

        # Insert the present directory first on the system path
        sys.path.insert(0, self.app_path)

        # Import the app from the present directory
        self.app_module = importlib.import_module("app")

        # Immediately clean up the entry to the system path (don't use "remove" because if the user has it in their
        # path, this'll be an unexpected side effect, and don't do it in cleanup in case the called code inserts a path)
        sys.path.pop(0)
        module_logger.debug(
            f"Imported app at app_path and cleaned up temporary modification to sys.path {self.app_path}"
        )

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Unload the imported module
        del sys.modules["app"]
        module_logger.debug(f"Deleted app from sys.modules and cleaned up (app_path {self.app_path})")

    @property
    def run(self):
        """ Returns the unwrapped run function from app.py in the application's root directory
        """
        return unwrap(self.app_module.run)
