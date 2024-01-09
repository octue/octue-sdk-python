import copy
import logging
import logging.handlers
import os
import re

import google.api_core.exceptions
from google import auth
from google.cloud import secretmanager
from jsonschema import ValidationError, validate as jsonschema_validate

import twined.exceptions
from octue import exceptions
from octue.app_loading import AppFrom
from octue.diagnostics import Diagnostics
from octue.log_handlers import AnalysisLogFormatterSwitcher
from octue.resources import Child
from octue.resources.analysis import CLASS_MAP, Analysis
from octue.resources.datafile import downloaded_files
from octue.utils import gen_uuid
from twined import Twine


SAVE_DIAGNOSTICS_OFF = "SAVE_DIAGNOSTICS_OFF"
SAVE_DIAGNOSTICS_ON_CRASH = "SAVE_DIAGNOSTICS_ON_CRASH"
SAVE_DIAGNOSTICS_ON = "SAVE_DIAGNOSTICS_ON"
SAVE_DIAGNOSTICS_MODES = {SAVE_DIAGNOSTICS_OFF, SAVE_DIAGNOSTICS_ON_CRASH, SAVE_DIAGNOSTICS_ON}


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
    :param str|None diagnostics_cloud_path: the path to a cloud directory to store diagnostics in the event that the service fails while processing a question (this includes the configuration, input values and manifest, and logs)
    :param str|None project_name: name of Google Cloud project to get credentials from
    :param str|None service_id: the ID of the service being run
    :param bool delete_local_files: if `True`, delete any files downloaded during the call to `Runner.run` once the analysis has finished
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
        diagnostics_cloud_path=None,
        project_name=None,
        service_id=None,
        service_registries=None,
        delete_local_files=True,
    ):
        self.app_source = app_src
        self.children = children

        if output_location and not re.match(r"^gs://[a-z\d][a-z\d_./-]*$", output_location):
            raise exceptions.InvalidInputException(
                "The output location must be a Google Cloud Storage path e.g. 'gs://bucket-name/output_directory'."
            )

        self.output_location = output_location

        # Get configuration before any transformations have been applied.
        self.diagnostics = Diagnostics(cloud_path=diagnostics_cloud_path)

        self.diagnostics.add_data(
            configuration_values=configuration_values,
            configuration_manifest=configuration_manifest,
        )

        self._storage_client = None

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
        self.service_registries = service_registries
        self.delete_local_files = delete_local_files
        self._project_name = project_name

    @classmethod
    def from_configuration(cls, service_configuration, app_configuration, project_name=None, service_id=None):
        """Instantiate a runner from a service and app configuration.

        :param octue.configuration.ServiceConfiguration service_configuration:
        :param octue.configuration.AppConfiguration app_configuration:
        :param str|None project_name: name of Google Cloud project to get credentials from
        :param str|None service_id: the ID of the service being run
        :return octue.runner.Runner: a runner configured with the given service and app configuration
        """
        return cls(
            app_src=service_configuration.app_source_path,
            twine=service_configuration.twine_path,
            configuration_values=app_configuration.configuration_values,
            configuration_manifest=app_configuration.configuration_manifest,
            children=app_configuration.children,
            output_location=app_configuration.output_location,
            diagnostics_cloud_path=service_configuration.diagnostics_cloud_path,
            project_name=project_name,
            service_id=service_id,
            service_registries=service_configuration.service_registries,
        )

    def __repr__(self):
        """Represent the runner as a string.

        :return str: the runner represented as a string.
        """
        return f"<{type(self).__name__}({self.service_id!r})>"

    def run(
        self,
        analysis_id=None,
        input_values=None,
        input_manifest=None,
        children=None,
        analysis_log_level=logging.INFO,
        analysis_log_handler=None,
        handle_monitor_message=None,
        save_diagnostics=SAVE_DIAGNOSTICS_ON_CRASH,
    ):
        """Run an analysis.

        :param str|None analysis_id: UUID of analysis
        :param str|dict|None input_values: the input_values strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
        :param str|dict|octue.resources.manifest.Manifest|None input_manifest: The input_manifest strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
        :param list(dict)|None children: a list of children to use instead of the children provided at instantiation. These should be in the same format as in an app's app configuration file and have the same keys.
        :param str analysis_log_level: the level below which to ignore log messages
        :param logging.Handler|None analysis_log_handler: the logging.Handler instance which will be used to handle logs for this analysis run. Handlers can be created as per the logging cookbook https://docs.python.org/3/howto/logging-cookbook.html but should use the format defined above in LOG_FORMAT.
        :param callable|None handle_monitor_message: a function that sends monitor messages to the parent that requested the analysis
        :param str save_diagnostics: must be one of {"SAVE_DIAGNOSTICS_OFF", "SAVE_DIAGNOSTICS_ON_CRASH", "SAVE_DIAGNOSTICS_ON"}; if turned on, allow the input values and manifest (and its datasets) to be saved either all the time or just if the analysis fails
        :return octue.resources.analysis.Analysis:
        """
        if save_diagnostics not in SAVE_DIAGNOSTICS_MODES:
            raise ValueError(
                f"`save_diagnostics` must be one of {SAVE_DIAGNOSTICS_MODES!r}; received {save_diagnostics!r}."
            )

        # Get inputs before any transformations have been applied.
        self.diagnostics.add_data(
            analysis_id=analysis_id,
            input_values=input_values,
            input_manifest=input_manifest,
        )

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
            inputs["children"] = self._instantiate_children(inputs["children"])

        outputs_and_monitors = self.twine.prepare("monitor_message", "output_values", "output_manifest", cls=CLASS_MAP)

        if analysis_log_handler:
            extra_log_handlers = [analysis_log_handler]
        else:
            extra_log_handlers = []

        analysis_id = str(analysis_id) if analysis_id else gen_uuid()

        # Temporarily replace the root logger's handlers with a `StreamHandler` and the analysis log handler that
        # include the analysis ID in the logging metadata.
        with AnalysisLogFormatterSwitcher(
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
                self._load_and_run_app(analysis)

            except ModuleNotFoundError as e:
                raise ModuleNotFoundError(f"{e.msg} in {os.path.abspath(self.app_source)!r}.")

            except Exception as analysis_error:
                if save_diagnostics in {SAVE_DIAGNOSTICS_ON_CRASH, SAVE_DIAGNOSTICS_ON}:
                    self.diagnostics.upload()

                raise analysis_error

            finally:
                for i, thread in enumerate(analysis._periodic_monitor_message_sender_threads):
                    thread.cancel()
                    logger.debug("Periodic monitor message thread %d stopped.", i)

            if not analysis.finalised:
                analysis.finalise()

            if save_diagnostics == SAVE_DIAGNOSTICS_ON:
                self.diagnostics.upload()

            if self.delete_local_files and downloaded_files:
                logger.warning(
                    "Deleting files downloaded during analysis. This is not thread-safe - set "
                    "`delete_local_files=False` at instantiation of `Runner` to switch this off."
                )

                for path in downloaded_files:
                    logger.debug("Deleting downloaded file at %r.", path)

                    try:
                        os.remove(path)
                    except FileNotFoundError:
                        logger.debug("Couldn't delete %r - it was already deleted.", path)

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
                project=project_name,
                secret=credential["name"],
                secret_version="latest",
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
                        f"{file.name!r}. Add the property to the datafile as a tag to fix this."
                    )

                    raise twined.exceptions.invalid_contents_map[manifest_kind](message)

    def _instantiate_children(self, serialised_children):
        """Instantiate children from their serialised form (e.g. as given in the app configuration) so they are ready
        to be asked questions. For diagnostics, each child's `ask` method is wrapped so the runner can record the
        questions asked by the app, the responses received to each question, and the order the questions are asked in.

        :param list(dict) serialised_children: serialised children from e.g. the app configuration file
        :return dict: a mapping of child keys to `octue.resources.child.Child` instances
        """
        children = {}

        for uninstantiated_child in serialised_children:
            child = Child(
                id=uninstantiated_child["id"],
                backend=uninstantiated_child["backend"],
                internal_service_name=self.service_id,
                service_registries=self.service_registries,
            )

            child.ask = self._add_child_question_and_response_recording(child, uninstantiated_child["key"])
            children[uninstantiated_child["key"]] = child

        return children

    def _add_child_question_and_response_recording(self, child, key):
        """Add question and response recording to the `ask` method of the given child. This allows the runner to record
        the questions asked by the app, the responses received to each question, and the order the questions are asked
        in for diagnostics.

        :param octue.resources.child.Child child: the child to add question and response recording to
        :param str key: the key used to identify the child within the service
        :return callable: the wrapped `Child.ask` method
        """
        # Copy the `ask` method to avoid an infinite recursion.
        original_ask_method = copy.copy(child.ask)

        def wrapper(*args, **kwargs):
            # Convert args to kwargs so all inputs to the `ask` method can be recorded whether they're provided
            # positionally or as keyword arguments.
            kwargs.update(dict(zip(original_ask_method.__func__.__code__.co_varnames, args)))

            try:
                return original_ask_method(**kwargs)
            finally:
                self.diagnostics.add_question(
                    {"id": child.id, "key": key, **kwargs, "messages": child.received_messages}
                )

        return wrapper

    def _load_and_run_app(self, analysis):
        """Load and run the app on the given analysis object.

        :param octue.resources.analysis.Analysis analysis: the analysis object containing the configuration and inputs to run the app on
        :return None:
        """
        # App as a class that takes "analysis" as a constructor argument and contains a method named "run" that
        # takes no arguments.
        if isinstance(self.app_source, type):
            self.app_source(analysis).run()
            return

        # App as a module containing a function named "run" that takes "analysis" as an argument.
        if hasattr(self.app_source, "run"):
            self.app_source.run(analysis)
            return

        # App as a string path to a module containing a class named "App" or a function named "run". The same
        # other specifications apply as described above.
        if isinstance(self.app_source, str):
            with AppFrom(self.app_source) as app:
                if hasattr(app.app_module, "App"):
                    app.app_module.App(analysis).run()
                else:
                    app.app_module.run(analysis)
            return

        # App as a function that takes "analysis" as an argument.
        self.app_source(analysis)
