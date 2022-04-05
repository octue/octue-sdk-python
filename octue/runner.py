import importlib
import logging
import os
import sys

import google.api_core.exceptions
from google import auth
from google.cloud import secretmanager
from jsonschema import ValidationError, validate as jsonschema_validate

import twined.exceptions
from octue.log_handlers import apply_log_handler, create_octue_formatter, get_log_record_attributes_for_environment
from octue.resources import Child
from octue.resources.analysis import CLASS_MAP, Analysis
from octue.utils import gen_uuid
from twined import Twine


logger = logging.getLogger(__name__)


class Runner:
    """Runs analyses in the app framework

    The Runner class provides a set of configuration parameters for use by your application, together with a range of
    methods for managing input and output file parsing as well as controlling logging.

    :param Union[AppFrom, callable, str] app_src: Either an instance of the AppFrom manager class which has a run() method, or a function which accepts a single parameter (the instantiated analysis), or a string pointing to an application folder (which should contain an 'app.py' function like the templates)
    :param str|twined.Twine twine: path to the twine file, a string containing valid twine json, or a Twine instance
    :param str|dict|_io.TextIOWrapper|None configuration_values: The strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
    :param str|dict|_io.TextIOWrapper|None configuration_manifest: The strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
    :param Union[str, path-like, None] output_manifest_path: Path where output data will be written
    :param Union[str, dict, None] children: The children strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
    :param bool skip_checks: If true, skip the check that all files in the manifest are present on disc - this can be an extremely long process for large datasets.
    :param str|None project_name: name of Google Cloud project to get credentials from
    :return None:
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
        project_name=None,
    ):
        self.app_src = app_src
        self.output_manifest_path = output_manifest_path
        self.children = children
        self.skip_checks = skip_checks

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

        self._project_name = project_name

    def run(
        self,
        analysis_id=None,
        input_values=None,
        input_manifest=None,
        analysis_log_level=logging.INFO,
        analysis_log_handler=None,
        handle_monitor_message=None,
    ):
        """Run an analysis.

        :param str|None analysis_id: UUID of analysis
        :param Union[str, dict, None] input_values: the input_values strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
        :param Union[str, octue.resources.manifest.Manifest, None] input_manifest: The input_manifest strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
        :param str analysis_log_level: the level below which to ignore log messages
        :param logging.Handler|None analysis_log_handler: the logging.Handler instance which will be used to handle logs for this analysis run. Handlers can be created as per the logging cookbook https://docs.python.org/3/howto/logging-cookbook.html but should use the format defined above in LOG_FORMAT.
        :param callable|None handle_monitor_message: a function that sends monitor messages to the parent that requested the analysis
        :return None:
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

        for manifest_strand in self.twine.available_manifest_strands:
            if manifest_strand == "output_manifest":
                continue

            self._validate_dataset_file_tags(manifest_kind=manifest_strand, manifest=inputs[manifest_strand])

        if inputs["children"] is not None:
            inputs["children"] = {
                child["key"]: Child(name=child["key"], id=child["id"], backend=child["backend"])
                for child in inputs["children"]
            }

        outputs_and_monitors = self.twine.prepare("monitor_message", "output_values", "output_manifest", cls=CLASS_MAP)

        analysis_id = str(analysis_id) if analysis_id else gen_uuid()

        # Temporarily replace the root logger's handlers with a `StreamHandler` and the analysis log handler that
        # include the analysis ID in the logging metadata.
        with AnalysisLogHandlerSwitcher(
            analysis_id=analysis_id,
            logger=logging.getLogger(),
            analysis_log_level=analysis_log_level,
            extra_log_handlers=[analysis_log_handler],
        ):

            analysis = Analysis(
                id=analysis_id,
                twine=self.twine,
                handle_monitor_message=handle_monitor_message,
                skip_checks=self.skip_checks,
                **self.configuration,
                **inputs,
                **outputs_and_monitors,
            )

            try:
                # App as a class that takes "analysis" as a constructor argument and contains a method named "run" that
                # takes no arguments.
                if isinstance(self.app_src, type):
                    self.app_src(analysis).run()

                # App as a module containing a function named "run" that takes "analysis" as an argument.
                elif hasattr(self.app_src, "run"):
                    self.app_src.run(analysis)

                # App as a string path to a module containing a class named "App" or a function named "run". The same other
                # specifications apply as described above.
                elif isinstance(self.app_src, str):

                    with AppFrom(self.app_src) as app:
                        if hasattr(app.app_module, "App"):
                            app.app_module.App(analysis).run()
                        else:
                            app.run(analysis)

                # App as a function that takes "analysis" as an argument.
                else:
                    self.app_src(analysis)

            except ModuleNotFoundError as e:
                raise ModuleNotFoundError(f"{e.msg} in {os.path.abspath(self.app_src)!r}.")

            except Exception as e:
                logger.error(str(e))
                raise e

            return analysis

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

        google_cloud_credentials, project_name = auth.default()
        secrets_client = secretmanager.SecretManagerServiceClient(credentials=google_cloud_credentials)

        if google_cloud_credentials is None:
            project_name = self._project_name

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

    def _validate_dataset_file_tags(self, manifest_kind, manifest):
        """Validate the tags of the files of each dataset in the manifest against the file tags template in the
        corresponding dataset field in the given manifest field of the twine.

        :param str manifest_kind: the kind of manifest that's being validated (so the correct schema can be accessed)
        :param octue.resources.manifest.Manifest manifest: the manifest whose datasets' files are to be validated
        :return None:
        """
        # This is the manifest schema included in the twine.json file, not the schema for `manifest.json` files.
        manifest_schema = getattr(self.twine, manifest_kind)

        for dataset_name, dataset_schema in manifest_schema["datasets"].items():
            dataset = manifest.datasets.get(dataset_name)
            file_tags_template = dataset_schema.get("file_tags_template")

            # Allow optional datasets in future (not currently allowed by `twined`).
            if not (dataset and file_tags_template):
                continue

            for file in dataset.files:
                try:
                    jsonschema_validate(instance=dict(file.tags), schema=file_tags_template)
                except ValidationError as e:
                    raise twined.exceptions.invalid_contents_map[manifest_kind](str(e))


def unwrap(fcn):
    """Recurse through wrapping to get the raw function without decorators."""
    if hasattr(fcn, "__wrapped__"):
        return unwrap(fcn.__wrapped__)
    return fcn


class AppFrom:
    """A context manager that imports the module "app" from a file named "app.py" in the given directory on entry (by
    making a temporary addition to the system path) and unloads it (by deleting it from `sys.modules`) on exit. It will
    issue a warning if an existing module called "app" is already loaded. Usage example:

    ```python3
    with AppFrom('/path/to/dir') as app:
        Runner().run(app)
    ```

    :param str app_path: path to directory containing module named "app.py".
    :return None:
    """

    def __init__(self, app_path="."):
        self.app_path = os.path.abspath(os.path.normpath(app_path))
        logger.debug("Initialising AppFrom context at app_path %s", self.app_path)
        self.app_module = None

    def __enter__(self):
        # Warn on an app present on the system path
        if "app" in sys.modules.keys():
            logger.warning(
                "Module 'app' already on system path. Using 'AppFrom' context will yield unexpected results. Avoid "
                "using 'app' as a python module, except for your main entrypoint."
            )

        # Insert the present directory first on the system path.
        sys.path.insert(0, self.app_path)

        # Import the app from the present directory.
        self.app_module = importlib.import_module("app")

        # Immediately clean up the entry to the system path (don't use "remove" because if the user has it in their
        # path, this'll be an unexpected side effect, and don't do it in cleanup in case the called code inserts a path)
        sys.path.pop(0)
        logger.debug("Imported app at app_path and cleaned up temporary modification to sys.path %s", self.app_path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Unload the imported module.

        :return None:
        """
        try:
            del sys.modules["app"]
            logger.debug("Deleted app from sys.modules")

        except KeyError:
            context_manager_name = type(self).__name__
            logger.warning(
                f"Module 'app' was already removed from the system path prior to exiting the {context_manager_name} "
                f"context manager. Using the {context_manager_name} context may yield unexpected results."
            )

    @property
    def run(self):
        """Get the unwrapped run function from app.py in the application's root directory."""
        return unwrap(self.app_module.run)


class AnalysisLogHandlerSwitcher:
    """A context manager that, when activated, takes the given logger, removes its handlers, and adds a local handler
    and any other handlers provided to it. A formatter is applied to the handlers that includes the given analysis ID
    in the logging context. On leaving the context, the logger's initial handlers are restored to it and any that were
    added to it in the context are removed.

    :param str analysis_id:
    :param logger.Logger logger:
    :param str analysis_log_level:
    :param list(logging.Handler) extra_log_handlers:
    :return None:
    """

    def __init__(self, analysis_id, logger, analysis_log_level, extra_log_handlers=None):
        self.analysis_id = analysis_id
        self.logger = logger
        self.analysis_log_level = analysis_log_level
        self.extra_log_handlers = extra_log_handlers or []
        self.initial_handlers = []

    def __enter__(self):
        """Remove the initial handlers from the logger, create a formatter that includes the analysis ID, use the
        formatter on the local and extra handlers, and add these handlers to the logger.

        :return None:
        """
        self.initial_handlers = list(self.logger.handlers)
        self._remove_log_handlers()

        # Add the analysis ID to the logging metadata.
        log_record_attributes = get_log_record_attributes_for_environment() + [f"analysis-{self.analysis_id}"]
        formatter = create_octue_formatter(log_record_attributes=log_record_attributes)

        # Apply a local console `StreamHandler` to the logger.
        apply_log_handler(formatter=formatter, log_level=self.analysis_log_level)

        if not self.extra_log_handlers:
            return

        # Apply any other given handlers to the logger.
        for extra_handler in self.extra_log_handlers:
            apply_log_handler(handler=extra_handler, log_level=self.analysis_log_level, formatter=formatter)

    def __exit__(self, *args):
        """Remove the new handlers from the logger and re-add the initial handlers.

        :return None:
        """
        self._remove_log_handlers()

        for handler in self.initial_handlers:
            self.logger.addHandler(handler)

    def _remove_log_handlers(self):
        """Remove all handlers from the logger.

        :return None:
        """
        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)
