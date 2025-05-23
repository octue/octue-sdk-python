import logging
import os

import yaml

logger = logging.getLogger(__name__)


DEFAULT_SERVICE_CONFIGURATION_PATH = "octue.yaml"


class ServiceConfiguration:
    """A class containing the details needed to configure a service. The configuration values and manifest data must
    conform to the service's twine schema.

    :param str name: the name to give the service
    :param str namespace: the namespace for grouping the service with others (e.g. the name of an organisation or individual)
    :param str app_source_path: the path to the directory containing the app's source code
    :param str twine_path: the path to the twine file defining the schema for input, output, and configuration data for the service
    :param str|None diagnostics_cloud_path: the path to a cloud directory to store diagnostics (this includes the configuration, input values and manifest, and logs for each question)
    :param iter(dict)|None service_registries: the names and endpoints of the registries used to resolve service revisions when asking questions; these should be in priority order (highest priority first)
    :param str|None event_store_table_id: the full ID of the Google BigQuery table used as the event store e.g. "your-project.your-dataset.your-table"
    :param bool delete_local_files: if `True`, delete any files downloaded and temporary directories created during an analysis once it's finished
    :param str|dict|list|None configuration_values: values to configure the app
    :param str|dict|octue.resources.Manifest|None configuration_manifest: a manifest of datasets to configure the app
    :param str|list(dict)|None children: details of the children the app requires
    :param str|None output_location: the path to a cloud directory to save output datasets at
    :param bool use_signed_urls_for_output_datasets: if `True`, use signed URLs instead of cloud URIs for dataset paths in the output manifest
    :param str|None directory: if provided, find the app source and twine relative to this directory
    :return None:
    """

    def __init__(
        self,
        name,
        namespace,
        app_source_path=".",
        twine_path="twine.json",
        diagnostics_cloud_path=None,
        service_registries=None,
        event_store_table_id=None,
        delete_local_files=False,
        configuration_values=None,
        configuration_manifest=None,
        children=None,
        output_location=None,
        use_signed_urls_for_output_datasets=False,
        directory=None,
        **kwargs,
    ):
        self.name = name
        self.namespace = namespace
        self.diagnostics_cloud_path = diagnostics_cloud_path
        self.service_registries = service_registries
        self.event_store_table_id = event_store_table_id
        self.delete_local_files = delete_local_files

        # Values formerly from app configuration.
        self.configuration_values = configuration_values
        self.configuration_manifest = configuration_manifest
        self.children = children
        self.output_location = output_location
        self.use_signed_urls_for_output_datasets = use_signed_urls_for_output_datasets

        if directory:
            directory = os.path.abspath(directory)

            if app_source_path == ".":
                self.app_source_path = directory
            else:
                self.app_source_path = os.path.join(directory, app_source_path)

            self.twine_path = os.path.join(directory, twine_path)

        else:
            self.app_source_path = os.path.abspath(app_source_path)
            self.twine_path = os.path.abspath(twine_path)

        if kwargs:
            logger.warning(f"The following keyword arguments were not used by {type(self).__name__}: {kwargs!r}.")

    @classmethod
    def from_file(cls, path=None, allow_not_found=False):
        """Load a service configuration from a YAML file.

        :param str|None path: the path to the service configuration YAML file; if not provided, the `OCTUE_SERVICE_CONFIGURATION_PATH` environment variable is used if present, otherwise the local path `octue.yaml` is used
        :param bool allow_not_found: if `True`, return `None` instead of raising an error if a service configuration file isn't found
        :return ServiceConfiguration|None: the service configuration loaded from the file
        """
        path = path or os.environ.get("OCTUE_SERVICE_CONFIGURATION_PATH", DEFAULT_SERVICE_CONFIGURATION_PATH)

        try:
            with open(path) as f:
                raw_service_configuration = yaml.load(f, Loader=yaml.SafeLoader)
        except FileNotFoundError as error:
            if allow_not_found:
                return None
            else:
                raise error

        absolute_path = os.path.abspath(path)
        logger.info("Service configuration loaded from %r.", absolute_path)

        # Ignore services other than the first for now.
        return cls(**raw_service_configuration["services"][0], directory=os.path.dirname(absolute_path))

    def __repr__(self):
        """Represent the service configuration as a string.

        :return str: the service configuration as a string
        """
        return f"<{type(self).__name__}('{self.namespace}/{self.name}')>"
