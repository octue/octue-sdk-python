import copy
import tempfile
from unittest.mock import Mock, patch

from apache_beam.runners.dataflow.internal.apiclient import DataflowJobAlreadyExistsError

from octue.cloud.deployment.google.dataflow.deployer import (
    DEFAULT_DATAFLOW_DOCKERFILE_URL,
    DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION,
    DEFAULT_SETUP_FILE_PATH,
    OCTUE_SDK_PYTHON_IMAGE_URI,
    DataflowDeployer,
)
from octue.exceptions import DeploymentError
from tests.base import BaseTestCase


octue_configuration = {
    "services": [
        {
            "name": "test-service",
            "repository_name": "test-repository",
            "repository_owner": "octue",
            "project_name": "test-project",
            "region": "europe-west2",
            "branch_pattern": "my-branch",
            "service_account_email": "account@domain.com",
            "maximum_instances": 30,
            "machine_type": "e2-standard-2",
        }
    ]
}

service = octue_configuration["services"][0]

octue_configuration_with_cloud_build_path = {
    "services": [{**copy.copy(service), "cloud_build_configuration_path": "cloudbuild.yaml"}]
}

SERVICE_ID = "octue.services.0df08f9f-30ad-4db3-8029-ea584b4290b7"

EXPECTED_IMAGE_NAME = (
    f"eu.gcr.io/{service['project_name']}/{service['repository_name']}/" f"{service['name']}:$SHORT_SHA"
)

EXPECTED_CLOUD_BUILD_CONFIGURATION = {
    "steps": [
        {
            "id": "Get default Octue Dockerfile",
            "name": "alpine:latest",
            "args": ["wget", DEFAULT_DATAFLOW_DOCKERFILE_URL],
        },
        {
            "id": "Build image",
            "name": "gcr.io/cloud-builders/docker",
            "entrypoint": "bash",
            "args": [
                "-c",
                (
                    f"docker build '-t' {EXPECTED_IMAGE_NAME!r} --build-arg=SERVICE_ID={SERVICE_ID} "
                    f"--build-arg=SERVICE_NAME={service['name']} . '-f' Dockerfile"
                ),
            ],
        },
        {
            "id": "Push image",
            "name": "gcr.io/cloud-builders/docker",
            "args": ["push", EXPECTED_IMAGE_NAME],
        },
        {
            "id": "Deploy Dataflow job",
            "name": OCTUE_SDK_PYTHON_IMAGE_URI,
            "args": [
                "octue-app",
                "deploy",
                "dataflow",
                f"--service-id={SERVICE_ID}",
                "--update",
                "--dataflow-job-only",
                f"--image-uri={EXPECTED_IMAGE_NAME}",
            ],
        },
    ],
    "images": [EXPECTED_IMAGE_NAME],
}

EXPECTED_BUILD_TRIGGER_CREATION_COMMAND = [
    "gcloud",
    f"--project={service['project_name']}",
    "beta",
    "builds",
    "triggers",
    "create",
    "github",
    f"--name={service['name']}",
    f"--repo-name={service['repository_name']}",
    f"--repo-owner={service['repository_owner']}",
    f"--description=Build the {service['name']!r} service and deploy it to Dataflow.",
    f"--branch-pattern={service['branch_pattern']}",
]


class TestDataflowDeployer(BaseTestCase):
    def test_generate_cloud_build_configuration(self):
        """Test that a correct Google Cloud Build configuration is generated from the given `octue.yaml` file."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(octue_configuration, temporary_directory)
            deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

        deployer._generate_cloud_build_configuration()
        self.assertEqual(deployer.generated_cloud_build_configuration, EXPECTED_CLOUD_BUILD_CONFIGURATION)

    def test_deploy(self):
        """Test that the build trigger creation and run are requested correctly."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(octue_configuration, temporary_directory)
            deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

            with patch("subprocess.run", return_value=Mock(returncode=0)) as mock_run:
                mock_build_id = "my-build-id"

                with patch(
                    "json.loads",
                    return_value={
                        "metadata": {"build": {"images": [deployer.image_uri_template], "id": mock_build_id}},
                        "status": "SUCCESS",
                    },
                ):
                    temporary_file = tempfile.NamedTemporaryFile(delete=False)

                    with patch("tempfile.NamedTemporaryFile", return_value=temporary_file):
                        deployer.deploy()

            # Test the build trigger creation request.
            self.assertEqual(
                mock_run.call_args_list[0].args[0],
                EXPECTED_BUILD_TRIGGER_CREATION_COMMAND + [f"--inline-config={temporary_file.name}"],
            )

            # Test the build trigger run request.
            self.assertEqual(
                mock_run.call_args_list[1].args[0],
                [
                    "gcloud",
                    f"--project={service['project_name']}",
                    "--format=json",
                    "beta",
                    "builds",
                    "triggers",
                    "run",
                    service["name"],
                    "--branch=my-branch",
                ],
            )

            # Test waiting for the build trigger run to complete.
            self.assertEqual(
                mock_run.call_args_list[2].args[0],
                [
                    "gcloud",
                    f'--project={service["project_name"]}',
                    "--format=json",
                    "builds",
                    "describe",
                    mock_build_id,
                ],
            )

    def test_deploy_with_cloud_build_file_provided(self):
        """Test deploying to Dataflow with a `cloudbuild.yaml` path provided in the `octue.yaml` file"""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(
                octue_configuration_with_cloud_build_path,
                temporary_directory,
            )

            deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID, image_uri_template="blah")

            with patch("subprocess.run", return_value=Mock(returncode=0)) as mock_run:
                mock_build_id = "my-build-id"

                with patch(
                    "json.loads",
                    return_value={
                        "metadata": {"build": {"images": [deployer.image_uri_template], "id": mock_build_id}},
                        "status": "SUCCESS",
                    },
                ):
                    with patch("octue.cloud.deployment.google.dataflow.pipeline.DataflowRunner"):
                        deployer.deploy()

            # Test the build trigger creation request.
            self.assertEqual(
                mock_run.call_args_list[0].args[0],
                EXPECTED_BUILD_TRIGGER_CREATION_COMMAND
                + [
                    f"--build-config={octue_configuration_with_cloud_build_path['services'][0]['cloud_build_configuration_path']}"
                ],
            )

            # Test the build trigger run request.
            self.assertEqual(
                mock_run.call_args_list[1].args[0],
                [
                    "gcloud",
                    f"--project={octue_configuration_with_cloud_build_path['services'][0]['project_name']}",
                    "--format=json",
                    "beta",
                    "builds",
                    "triggers",
                    "run",
                    octue_configuration_with_cloud_build_path["services"][0]["name"],
                    "--branch=my-branch",
                ],
            )

            # Test waiting for the build trigger run to complete.
            self.assertEqual(
                mock_run.call_args_list[2].args[0],
                [
                    "gcloud",
                    f'--project={service["project_name"]}',
                    "--format=json",
                    "builds",
                    "describe",
                    mock_build_id,
                ],
            )

    def test_create_streaming_dataflow_job(self):
        """Test creating a streaming dataflow job directly."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(octue_configuration, temporary_directory)
            deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

            with patch(
                "octue.cloud.deployment.google.dataflow.pipeline.Topic",
                return_value=Mock(path="projects/my-project/topics/my-topic"),
            ):
                with patch("octue.cloud.deployment.google.dataflow.pipeline.DataflowRunner") as mock_runner:
                    deployer.create_streaming_dataflow_job(image_uri="my-image-uri")

            options = mock_runner.mock_calls[1].kwargs["options"].get_all_options()
            self.assertFalse(options["update"])
            self.assertTrue(options["streaming"])
            self.assertEqual(options["project"], service["project_name"])
            self.assertEqual(options["job_name"], service["name"])
            self.assertEqual(options["temp_location"], DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION)
            self.assertEqual(options["region"], service["region"])
            self.assertEqual(options["sdk_container_image"], "my-image-uri")
            self.assertEqual(options["setup_file"], DEFAULT_SETUP_FILE_PATH)

    def test_updating_streaming_dataflow_job(self):
        """Test updating an existing streaming dataflow job."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(octue_configuration, temporary_directory)
            deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

            with patch(
                "octue.cloud.deployment.google.dataflow.pipeline.Topic",
                return_value=Mock(path="projects/my-project/topics/my-topic"),
            ):
                with patch("octue.cloud.deployment.google.dataflow.pipeline.DataflowRunner") as mock_runner:
                    deployer.create_streaming_dataflow_job(image_uri="my-image-uri", update=True)

            options = mock_runner.mock_calls[1].kwargs["options"].get_all_options()
            self.assertTrue(options["update"])
            self.assertTrue(options["streaming"])
            self.assertEqual(options["project"], service["project_name"])
            self.assertEqual(options["job_name"], service["name"])
            self.assertEqual(options["temp_location"], DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION)
            self.assertEqual(options["region"], service["region"])
            self.assertEqual(options["sdk_container_image"], "my-image-uri")
            self.assertEqual(options["setup_file"], DEFAULT_SETUP_FILE_PATH)

    def test_create_streaming_dataflow_job_when_job_does_not_already_exist(self):
        """Test that attempting to deploy an update to a Dataflow job when a job with the name of service does not
        already exist results in the job deployment being retried with `update` set to `False`.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(octue_configuration, temporary_directory)
            deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

            with patch(
                "octue.cloud.deployment.google.dataflow.pipeline.Topic",
                return_value=Mock(path="projects/my-project/topics/my-topic"),
            ):
                with patch(
                    "octue.cloud.deployment.google.dataflow.pipeline.DataflowRunner.run_pipeline",
                    side_effect=[ValueError, None],
                ) as mock_runner:
                    deployer.create_streaming_dataflow_job(image_uri="my-image-uri", update=True)

            # Check that the first attempt was to update the service.
            first_attempt_options = mock_runner.mock_calls[0].kwargs["options"].get_all_options()
            self.assertTrue(first_attempt_options["update"])

            # Check that the second attempt was to create the service.
            second_attempt_options = mock_runner.mock_calls[1].kwargs["options"].get_all_options()
            self.assertFalse(second_attempt_options["update"])

    def test_deployment_error_raised_if_dataflow_job_already_exists(self):
        """Test that a deployment error is raised if a Dataflow job already exists with the same name as the service."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(octue_configuration, temporary_directory)
            deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

            with patch(
                "octue.cloud.deployment.google.dataflow.pipeline.Topic",
                return_value=Mock(path="projects/my-project/topics/my-topic"),
            ):
                with patch(
                    "octue.cloud.deployment.google.dataflow.pipeline.DataflowRunner.run_pipeline",
                    side_effect=DataflowJobAlreadyExistsError(),
                ):
                    with self.assertRaises(DeploymentError):
                        deployer.create_streaming_dataflow_job(image_uri="my-image-uri", update=True)
