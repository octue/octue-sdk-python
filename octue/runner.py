import importlib
import logging
import os
import sys
import google.api_core.exceptions
from google.cloud import secretmanager

from octue.cloud.credentials import GCPCredentialsManager
from octue.logging_handlers import apply_log_handler
from octue.resources import Child
from octue.resources.analysis import CLASS_MAP, Analysis
from octue.utils import gen_uuid
from twined import Twine


logger = logging.getLogger(__name__)


class Runner:
    """Runs analyses in the app framework

    The Runner class provides a set of configuration parameters for use by your application, together with a range of
    methods for managing input and output file parsing as well as controlling logging.

    :param str|twined.Twine twine: path to the twine file, a string containing valid twine json, or a Twine instance
    :param str|dict paths: If a string, contains a single path to an existing data directory where
        (if not already present), subdirectories 'configuration', 'input', 'tmp', 'log' and 'output' will be created. If a
        dict, it should contain all of those keys, with each of their values being a path to a directory (which will be
        recursively created if it doesn't exist)
    :param str|dict|_io.TextIOWrapper configuration_values: The strand data. Can be expressed as a string path of a
        *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json
        data or as an already-parsed dict.
    :param str|dict|_io.TextIOWrapper configuration_manifest: The strand data. Can be expressed as a string path of a
    *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or
    as an already-parsed dict.
    :param bool skip_file_checks: If true, skip the check that all files in the manifest are present on disc - this
        can be an extremely long process for large datasets.
    :param str project_name: name of Google Cloud project to get credentials from
    """

    def __init__(
        self,
        app_src,
        twine="twine.json",
        configuration_values=None,
        configuration_manifest=None,
        output_manifest_path=None,
        children=None,
        skip_checks=False,
        log_level=logging.INFO,
        handler=None,
        project_name=None,
    ):
        self.app_src = app_src
        self.output_manifest_path = output_manifest_path
        self.children = children
        self.skip_checks = skip_checks
        self._log_level = log_level
        self.handler = handler

        # Ensure the twine is present and instantiate it.
        if isinstance(twine, Twine):
            self.twine = twine
        else:
            self.twine = Twine(source=twine)

        logger.debug("Parsed twine with strands %r", self.twine.available_strands)

        # Validate and initialise configuration data
        self.configuration = self.twine.validate(
            configuration_values=configuration_values,
            configuration_manifest=configuration_manifest,
            cls=CLASS_MAP,
        )
        logger.debug("Configuration validated.")

        # Set path for configuration manifest.
        # TODO this is hacky, we need to rearchitect the twined validation so we can do this kind of thing in there
        self.configuration["configuration_manifest"] = self._update_manifest_path(
            self.configuration.get("configuration_manifest", None),
            configuration_manifest,
        )

        self._project_name = project_name

    def run(self, analysis_id=None, input_values=None, input_manifest=None):
        """Run an analysis

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
        if hasattr(self.twine, "credentials"):
            self._populate_environment_with_google_cloud_secrets()
            credentials = self.twine.credentials
        else:
            credentials = None

        inputs = self.twine.validate(
            input_values=input_values,
            input_manifest=input_manifest,
            credentials=credentials,
            children=self.children,
            cls=CLASS_MAP,
            allow_missing=False,
            allow_extra=False,
        )
        logger.debug("Inputs validated.")

        if inputs["children"] is not None:
            inputs["children"] = {
                child["key"]: Child(name=child["key"], id=child["id"], backend=child["backend"])
                for child in inputs["children"]
            }

        # TODO this is hacky, we need to rearchitect the twined validation so we can do this kind of thing in there
        inputs["input_manifest"] = self._update_manifest_path(
            inputs.get("input_manifest", None),
            input_manifest,
        )

        outputs_and_monitors = self.twine.prepare("monitors", "output_values", "output_manifest", cls=CLASS_MAP)

        # TODO this is hacky, we need to rearchitect the twined validation so we can do this kind of thing in there
        outputs_and_monitors["output_manifest"] = self._update_manifest_path(
            outputs_and_monitors.get("output_manifest", None),
            self.output_manifest_path,
        )

        analysis_id = str(analysis_id) if analysis_id else gen_uuid()

        analysis_logger = apply_log_handler(
            logger_name=f"{__name__} | analysis-{analysis_id}", handler=self.handler, log_level=self._log_level
        )

        analysis_logger.propagate = False

        analysis = Analysis(
            id=analysis_id,
            logger=analysis_logger,
            twine=self.twine,
            skip_checks=self.skip_checks,
            **self.configuration,
            **inputs,
            **outputs_and_monitors,
        )

        try:
            if hasattr(self.app_src, "run"):
                self.app_src.run(analysis)
            elif isinstance(self.app_src, str):
                with AppFrom(self.app_src) as app:
                    app.run(analysis)
            else:
                self.app_src(analysis)

        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(f"{e.msg} in {os.path.abspath(self.app_src)!r}.")

        except Exception as e:
            analysis_logger.error(str(e))
            raise e

        return analysis

    @staticmethod
    def _update_manifest_path(manifest, pathname):
        """A Quick hack to stitch the new Pathable functionality in the 0.1.4 release into the CLI and runner.

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

    def _populate_environment_with_google_cloud_secrets(self):
        """Get any secrets specified in the credentials strand from Google Cloud Secret Manager and put them in the
        local environment, ready for use by the runner.

        :return None:
        """
        missing_credentials = tuple(
            credential for credential in self.twine.credentials if credential["name"] not in os.environ
        )

        if not missing_credentials:
            return

        google_cloud_credentials = GCPCredentialsManager().get_credentials()
        secrets_client = secretmanager.SecretManagerServiceClient(credentials=google_cloud_credentials)

        if google_cloud_credentials is None:
            project_name = self._project_name
        else:
            project_name = google_cloud_credentials.project_id

        for credential in missing_credentials:
            secret_path = secrets_client.secret_version_path(
                project=project_name, secret=credential["name"], secret_version="latest"
            )

            try:
                secret = secrets_client.access_secret_version(name=secret_path).payload.data.decode("UTF-8")
            except google.api_core.exceptions.NotFound:
                # No need to raise an error here as the Twine validation that follows will do so.
                continue

            os.environ[credential["name"]] = secret


def unwrap(fcn):
    """Recurse through wrapping to get the raw function without decorators."""
    if hasattr(fcn, "__wrapped__"):
        return unwrap(fcn.__wrapped__)
    return fcn


class AppFrom:
    """Context manager that imports module 'app' from user's code base at a location app_path.

     The manager will issue a warning if an existing module called "app" is already loaded.

     The manager makes a temporary addition to the system path (to ensure app is loaded from the correct path)

     The manager will unload the module (by deleting it from sys.modules) on exit, enabling

    with AppFrom('/path/to/dir') as app:
        Runner().run(app)

    """

    def __init__(self, app_path="."):
        self.app_path = os.path.abspath(os.path.normpath(app_path))
        logger.debug("Initialising AppFrom context at app_path %s", self.app_path)
        self.app_module = None

    def __enter__(self):
        # Warn on an app present on the system path
        if "app" in sys.modules.keys():
            logger.warning(
                "Module 'app' already on system path. Using 'AppFrom' context will yield unexpected results. Avoid using 'app' as a python module, except for your main entrypoint"
            )

        # Insert the present directory first on the system path
        sys.path.insert(0, self.app_path)

        # Import the app from the present directory
        self.app_module = importlib.import_module("app")

        # Immediately clean up the entry to the system path (don't use "remove" because if the user has it in their
        # path, this'll be an unexpected side effect, and don't do it in cleanup in case the called code inserts a path)
        sys.path.pop(0)
        logger.debug("Imported app at app_path and cleaned up temporary modification to sys.path %s", self.app_path)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Unload the imported module
        del sys.modules["app"]
        logger.debug("Deleted app from sys.modules and cleaned up (app_path %s)", self.app_path)

    @property
    def run(self):
        """Returns the unwrapped run function from app.py in the application's root directory"""
        return unwrap(self.app_module.run)
