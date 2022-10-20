import importlib
import json
import logging
import logging.handlers
import os
import re
import sys

import google.api_core.exceptions
from google import auth
from google.cloud import secretmanager
from jsonschema import ValidationError, validate as jsonschema_validate

import twined.exceptions
from octue import exceptions
from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.log_handlers import apply_log_handler, create_octue_formatter, get_log_record_attributes_for_environment
from octue.resources import Child
from octue.resources.analysis import CLASS_MAP, Analysis
from octue.utils import gen_uuid
from octue.utils.encoders import OctueJSONEncoder
from twined import Twine


logger = logging.getLogger(__name__)


class Runner:
    """A runner of analyses for a given service.

    The ``Runner`` class provides a set of configuration parameters for use by your application, together with a range
    of methods for managing input and output file parsing as well as controlling logging.

    :param callable|type|module|str app_src: either a function that accepts an Octue analysis, a class with a ``run`` method that accepts an Octue analysis, or a path to a directory containing an ``app.py`` file containing one of these
    :param str|dict|twined.Twine twine: path to the twine file, a string containing valid twine json, or a Twine instance
    :param str|dict|None configuration_values: The strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
    :param str|dict|None configuration_manifest: The strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
    :param str|list(dict)|None children: The children strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
    :param str|None output_location: the path to a cloud directory to save output datasets at
    :param str|None crash_diagnostics_cloud_path: the path to a cloud directory to store crash diagnostics in the event that the service fails while processing a question (this includes the configuration, input values and manifest, and logs)
    :param str|None project_name: name of Google Cloud project to get credentials from
    :param str|None service_id: the ID of the service being run
    :return None:
    """

    def __init__(
        self,
        app_src,
        twine="twine.json",
        configuration_values=None,
        configuration_manifest=None,
        children=None,
        output_location=None,
        crash_diagnostics_cloud_path=None,
        project_name=None,
        service_id=None,
    ):
        self.app_source = app_src
        self.children = children

        if output_location and not re.match(r"^gs://[a-z\d][a-z\d_./-]*$", output_location):
            raise exceptions.InvalidInputException(
                "The output location must be a Google Cloud Storage path e.g. 'gs://bucket-name/output_directory'."
            )

        self.output_location = output_location

        self.crash_diagnostics_cloud_path = crash_diagnostics_cloud_path

        # Ensure the twine is present and instantiate it.
        if isinstance(twine, Twine):
            self.twine = twine
        else:
            self.twine = Twine(source=twine)

        logger.debug("Parsed twine with strands %r", self.twine.available_strands)

        # Validate and initialise configuration data.
        self.configuration = self.twine.validate(
            configuration_values=configuration_values,
            configuration_manifest=configuration_manifest,
            cls=CLASS_MAP,
        )
        logger.debug("Configuration validated.")

        self.service_id = service_id
        self._project_name = project_name

    def run(
        self,
        analysis_id=None,
        input_values=None,
        input_manifest=None,
        children=None,
        analysis_log_level=logging.INFO,
        analysis_log_handler=None,
        handle_monitor_message=None,
        allow_save_diagnostics_data_on_crash=True,
        sent_messages=None,
    ):
        """Run an analysis.

        :param str|None analysis_id: UUID of analysis
        :param str|dict|None input_values: the input_values strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
        :param str|dict|octue.resources.manifest.Manifest|None input_manifest: The input_manifest strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
        :param list(dict)|None children: a list of children to use instead of the children provided at instantiation. These should be in the same format as in an app's app configuration file and have the same keys.
        :param str analysis_log_level: the level below which to ignore log messages
        :param logging.Handler|None analysis_log_handler: the logging.Handler instance which will be used to handle logs for this analysis run. Handlers can be created as per the logging cookbook https://docs.python.org/3/howto/logging-cookbook.html but should use the format defined above in LOG_FORMAT.
        :param callable|None handle_monitor_message: a function that sends monitor messages to the parent that requested the analysis
        :param bool allow_save_diagnostics_data_on_crash: if `True`, allow the input values and manifest (and its datasets) to be saved if the analysis fails
        :param list|None sent_messages: the list of messages sent by the service running this runner (this should update in real time) to save if crash diagnostics are enabled
        :return octue.resources.analysis.Analysis:
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
            children=children or self.children,
            cls=CLASS_MAP,
            allow_missing=False,
            allow_extra=False,
        )
        logger.debug("Inputs validated.")

        inputs_and_configuration = {**self.configuration, **inputs}

        for manifest_strand in self.twine.available_manifest_strands:
            if manifest_strand == "output_manifest":
                continue

            self._validate_dataset_file_tags(
                manifest_kind=manifest_strand,
                manifest=inputs_and_configuration[manifest_strand],
            )

        if inputs["children"] is not None:
            inputs["children"] = {
                child["key"]: Child(
                    id=child["id"],
                    backend=child["backend"],
                    internal_service_name=self.service_id,
                )
                for child in inputs["children"]
            }

        outputs_and_monitors = self.twine.prepare("monitor_message", "output_values", "output_manifest", cls=CLASS_MAP)

        if analysis_log_handler:
            extra_log_handlers = [analysis_log_handler]
        else:
            extra_log_handlers = []

        analysis_id = str(analysis_id) if analysis_id else gen_uuid()

        # Temporarily replace the root logger's handlers with a `StreamHandler` and the analysis log handler that
        # include the analysis ID in the logging metadata.
        with AnalysisLogHandlerSwitcher(
            analysis_id=analysis_id,
            logger=logging.getLogger(),
            analysis_log_level=analysis_log_level,
            extra_log_handlers=extra_log_handlers,
        ):

            analysis = Analysis(
                id=analysis_id,
                twine=self.twine,
                handle_monitor_message=handle_monitor_message,
                output_location=self.output_location,
                **self.configuration,
                **inputs,
                **outputs_and_monitors,
            )

            try:
                # App as a class that takes "analysis" as a constructor argument and contains a method named "run" that
                # takes no arguments.
                if isinstance(self.app_source, type):
                    self.app_source(analysis).run()

                # App as a module containing a function named "run" that takes "analysis" as an argument.
                elif hasattr(self.app_source, "run"):
                    self.app_source.run(analysis)

                # App as a string path to a module containing a class named "App" or a function named "run". The same
                # other specifications apply as described above.
                elif isinstance(self.app_source, str):

                    with AppFrom(self.app_source) as app:
                        if hasattr(app.app_module, "App"):
                            app.app_module.App(analysis).run()
                        else:
                            app.app_module.run(analysis)

                # App as a function that takes "analysis" as an argument.
                else:
                    self.app_source(analysis)

            except ModuleNotFoundError as e:
                raise ModuleNotFoundError(f"{e.msg} in {os.path.abspath(self.app_source)!r}.")

            except Exception as analysis_error:
                logger.error(str(analysis_error))

                if allow_save_diagnostics_data_on_crash:
                    if not self.crash_diagnostics_cloud_path:
                        logger.warning(
                            "Cannot save crash diagnostics as the child doesn't have the "
                            "`crash_diagnostics_cloud_path` field set in its service configuration (`octue.yaml` file)."
                        )

                    else:
                        logger.warning("Saving crash diagnostics to %r.", self.crash_diagnostics_cloud_path)

                        try:
                            self._save_crash_diagnostics_data(analysis, sent_messages)
                            logger.warning("Crash diagnostics saved.")
                        except Exception as crash_diagnostics_save_error:
                            logger.error("Failed to save crash diagnostics.")
                            raise crash_diagnostics_save_error

                raise analysis_error

            if not analysis.finalised:
                analysis.finalise()

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
                    message = (
                        e.message + f" for files in the {dataset_name!r} dataset. The affected datafile is "
                        f"{file.path!r}. Add the property to the datafile as a tag to fix this."
                    )

                    raise twined.exceptions.invalid_contents_map[manifest_kind](message)

    def _save_crash_diagnostics_data(self, analysis, sent_messages):
        """Save the values, manifests, and datasets for the analysis configuration and inputs to the crash diagnostics
        cloud path.

        :param octue.resources.analysis.Analysis analysis:
        :param list|None sent_messages: the list of messages sent by the service running this runner (this should update in real time) to save if crash diagnostics are enabled
        :return None:
        """
        storage_client = GoogleCloudStorageClient()
        question_diagnostics_path = storage.path.join(self.crash_diagnostics_cloud_path, analysis.id)

        for data_type in ("configuration", "input"):

            # Upload the configuration and input values.
            values_type = f"{data_type}_values"

            if getattr(analysis, values_type):
                storage_client.upload_from_string(
                    json.dumps(getattr(analysis, values_type), cls=OctueJSONEncoder),
                    cloud_path=storage.path.join(question_diagnostics_path, f"{values_type}.json"),
                )

            # Upload the configuration and input manifests.
            manifest_type = f"{data_type}_manifest"

            if getattr(analysis, manifest_type):
                manifest = getattr(analysis, manifest_type)

                for name, dataset in manifest.datasets.items():
                    dataset.upload(storage.path.join(question_diagnostics_path, f"{manifest_type}_datasets", name))

                manifest.to_cloud(storage.path.join(question_diagnostics_path, f"{manifest_type}.json"))

        # Upload the crash diagnostics events record.
        storage_client.upload_from_string(
            string=json.dumps(sent_messages or [], cls=OctueJSONEncoder),
            cloud_path=storage.path.join(question_diagnostics_path, "messages.json"),
        )


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
        octue_formatter = create_octue_formatter(
            get_log_record_attributes_for_environment(),
            [f"analysis-{self.analysis_id}"],
        )

        # Apply a local console `StreamHandler` to the logger.
        apply_log_handler(formatter=octue_formatter, log_level=self.analysis_log_level)

        if not self.extra_log_handlers:
            return

        uncoloured_octue_formatter = create_octue_formatter(
            get_log_record_attributes_for_environment(),
            [f"analysis-{self.analysis_id}"],
            use_colour=False,
        )

        # Apply any other given handlers to the logger.
        for extra_handler in self.extra_log_handlers:

            # Apply an uncoloured log formatter to any file log handlers to keep them readable (i.e. to avoid the ANSI
            # escape codes).
            if type(extra_handler).__name__ == "FileHandler":
                apply_log_handler(
                    handler=extra_handler,
                    log_level=self.analysis_log_level,
                    formatter=uncoloured_octue_formatter,
                )
                continue

            if (
                type(extra_handler).__name__ == "MemoryHandler"
                and getattr(extra_handler, "target")
                and type(getattr(extra_handler, "target")).__name__ == "FileHandler"
            ):
                apply_log_handler(
                    handler=extra_handler.target,
                    log_level=self.analysis_log_level,
                    formatter=uncoloured_octue_formatter,
                )
                continue

            apply_log_handler(handler=extra_handler, log_level=self.analysis_log_level, formatter=octue_formatter)

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
