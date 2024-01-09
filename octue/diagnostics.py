import copy
import json
import logging

import coolname

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.resources import Dataset
from octue.utils.encoders import OctueJSONEncoder


logger = logging.getLogger(__name__)


class Diagnostics:
    """A handler for question diagnostics that allows uploading of explicitly added configuration and input data and any
    questions asked to other services.

    :param str cloud_path: the cloud path of a directory to upload any added data into
    :return None:
    """

    def __init__(self, cloud_path):
        self.cloud_path = cloud_path
        self.analysis_id = None
        self.configuration_values = None
        self.configuration_manifest = None
        self.input_values = None
        self.input_manifest = None
        self.questions = []
        self._storage_client = GoogleCloudStorageClient()

    def add_data(
        self,
        analysis_id=None,
        configuration_values=None,
        configuration_manifest=None,
        input_values=None,
        input_manifest=None,
    ):
        """Add an analysis ID, configuration values, a configuration manifest, input values, and/or an input manifest to
        the diagnostics. The values and manifests are deep-copied before being added. This method can be called
        multiple times as data becomes available. Calling again with the same keyword arguments will overwrite any data
        of that type added previously.

        :param str analysis_id: the ID of the analysis to save diagnostics for
        :param any configuration_values: configuration values to save
        :param any configuration_manifest: a configuration manifest to save
        :param any input_values: input values to save
        :param any input_manifest: an input manifest to save
        :return None:
        """
        if analysis_id:
            self.analysis_id = analysis_id

        if configuration_values:
            self.configuration_values = copy.deepcopy(configuration_values)

        if configuration_manifest:
            self.configuration_manifest = copy.deepcopy(configuration_manifest)

        if input_values:
            self.input_values = copy.deepcopy(input_values)

        if input_manifest:
            self.input_manifest = copy.deepcopy(input_manifest)

    def add_question(self, question):
        """Add a question to the list of questions to save.

        :param dict question: the question to add
        :return None:
        """
        self.questions.append(question)

    def upload(self):
        """Check that a cloud path has been provided before uploading any added data to the diagnostics cloud
        path. Any errors encountered during upload are caught and logged.

        :return None:
        """
        if not self.cloud_path:
            logger.warning(
                "Cannot upload diagnostics as the child doesn't have the `diagnostics_cloud_path` field set in its "
                "service configuration (`octue.yaml` file)."
            )
            return

        if not self.analysis_id:
            self.analysis_id = coolname.generate_slug(3)

        try:
            self._upload()
            logger.info("Diagnostics uploaded.")
        except Exception:
            logger.exception("Failed to upload diagnostics.")

    def _upload(self):
        """Upload any added data to the diagnostics cloud path.

        :return None:
        """
        question_diagnostics_path = storage.path.join(self.cloud_path, self.analysis_id)
        logger.warning("App failed - saving diagnostics to %r.", question_diagnostics_path)

        for data_type in ("configuration", "input"):
            values_type = f"{data_type}_values"
            values = getattr(self, values_type)

            if values is not None:
                if isinstance(values, str):
                    setattr(self, values_type, self._attempt_deserialise_json(values))

                self._upload_values(values_type, question_diagnostics_path)

            manifest_type = f"{data_type}_manifest"
            manifest = getattr(self, manifest_type)

            if manifest is not None:
                if isinstance(manifest, str):
                    setattr(self, manifest_type, self._attempt_deserialise_json(manifest))

                self._upload_manifest(manifest_type, question_diagnostics_path)

        # Upload the messages received from any children.
        self._storage_client.upload_from_string(
            string=json.dumps(self.questions, cls=OctueJSONEncoder),
            cloud_path=storage.path.join(question_diagnostics_path, "questions.json"),
        )

    @staticmethod
    def _attempt_deserialise_json(string):
        """Attempt to deserialise the given string from JSON. If deserialisation fails, the original string is returned.

        :param str string: the string to attempt to deserialise
        :return any: the deserialised python object or the original string
        """
        try:
            return json.loads(string)
        except json.decoder.JSONDecodeError:
            return string

    def _upload_values(self, values_type, question_diagnostics_path):
        """Upload the values of the given type as part of the diagnostics.

        :param str values_type: one of "configuration_values" or "input_values"
        :param str question_diagnostics_path: the path to a cloud directory to upload the values into
        :return None:
        """
        values = getattr(self, values_type)

        self._storage_client.upload_from_string(
            json.dumps(values, cls=OctueJSONEncoder),
            cloud_path=storage.path.join(question_diagnostics_path, f"{values_type}.json"),
        )

    def _upload_manifest(self, manifest_type, question_diagnostics_path):
        """Upload the serialised manifest of the given type and its datasets as part of the diagnostics.

        :param str manifest_type: one of "configuration_manifest" or "input_manifest"
        :param str question_diagnostics_path: the path to a cloud directory to upload the manifest into
        :return None:
        """
        manifest = getattr(self, manifest_type)

        # Upload each dataset and update its path in the manifest.
        for dataset_name, dataset_path in manifest["datasets"].items():

            # Handle manifests containing serialised datasets instead of just the datasets' paths. Datasets can be in
            # this state when serialised if they were instantiated using the `files` argument.
            if isinstance(dataset_path, dict):
                dataset_path = dataset_path["path"]

            new_dataset_path = storage.path.join(
                question_diagnostics_path,
                f"{manifest_type}_datasets",
                dataset_name,
            )

            Dataset(dataset_path).upload(new_dataset_path)
            manifest["datasets"][dataset_name] = new_dataset_path

        # Upload the serialised manifest.
        self._storage_client.upload_from_string(
            json.dumps(manifest, cls=OctueJSONEncoder),
            cloud_path=storage.path.join(question_diagnostics_path, f"{manifest_type}.json"),
        )
