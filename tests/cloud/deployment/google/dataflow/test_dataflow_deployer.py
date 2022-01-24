import tempfile
from unittest.mock import Mock, patch

from octue.cloud.deployment.google.dataflow.deployer import (
    DEFAULT_DATAFLOW_DOCKERFILE_URL,
    DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION,
    DEFAULT_SETUP_FILE_PATH,
    DataflowDeployer,
)
from tests.base import BaseTestCase


octue_configuration = {
    "name": "test-service",
    "repository_name": "test-repository",
    "repository_owner": "octue",
    "project_name": "test-project",
    "region": "europe-west2",
    "branch_pattern": "my-branch",
}

SERVICE_ID = "octue.services.0df08f9f-30ad-4db3-8029-ea584b4290b7"

MOCK_HEAD_COMMIT_SHORT_HASH = "3748907"

EXPECTED_IMAGE_NAME = (
    f"eu.gcr.io/{octue_configuration['project_name']}/{octue_configuration['repository_name']}/"
    f"{octue_configuration['name']}:{MOCK_HEAD_COMMIT_SHORT_HASH}"
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
            "args": [
                "build",
                *[f"--build-arg=SERVICE_ID={SERVICE_ID}", f"--build-arg=SERVICE_NAME={octue_configuration['name']}"],
                "-t",
                EXPECTED_IMAGE_NAME,
                ".",
                "-f",
                "Dockerfile",
            ],
        },
        {
            "id": "Push image",
            "name": "gcr.io/cloud-builders/docker",
            "args": ["push", EXPECTED_IMAGE_NAME],
        },
    ],
    "images": [EXPECTED_IMAGE_NAME],
}

EXPECTED_BUILD_TRIGGER_CREATION_COMMAND = [
    "gcloud",
    f"--project={octue_configuration['project_name']}",
    "beta",
    "builds",
    "triggers",
    "create",
    "github",
    f"--name={octue_configuration['name']}",
    f"--repo-name={octue_configuration['repository_name']}",
    f"--repo-owner={octue_configuration['repository_owner']}",
    f"--description=Build the {octue_configuration['name']!r} service and deploy it to Dataflow.",
    f"--branch-pattern={octue_configuration['branch_pattern']}",
]


class TestDataflowDeployer(BaseTestCase):
    def test_generate_cloud_build_configuration(self):
        """Test that a correct Google Cloud Build configuration is generated from the given `octue.yaml` file."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(octue_configuration, temporary_directory)

            with patch(
                "tests.cloud.deployment.google.dataflow.test_dataflow_deployer.DataflowDeployer._get_short_head_commit_hash",
                return_value=MOCK_HEAD_COMMIT_SHORT_HASH,
            ):
                deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

        deployer._generate_cloud_build_configuration()
        self.assertEqual(deployer.generated_cloud_build_configuration, EXPECTED_CLOUD_BUILD_CONFIGURATION)

    def test_deploy(self):
        """Test that the build trigger creation, build trigger run, and streaming Dataflow job deployment are requested
        correctly.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(octue_configuration, temporary_directory)
            deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

            with patch("subprocess.run", return_value=Mock(returncode=0)) as mock_run:
                mock_build_id = "my-build-id"

                with patch(
                    "json.loads",
                    return_value={
                        "metadata": {"build": {"images": ["my-image"], "id": mock_build_id}},
                        "status": "SUCCESS",
                    },
                ):
                    temporary_file = tempfile.NamedTemporaryFile(delete=False)

                    with patch("tempfile.NamedTemporaryFile", return_value=temporary_file):
                        with patch("octue.cloud.deployment.google.dataflow.pipeline.DataflowRunner") as mock_runner:
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
                    f"--project={octue_configuration['project_name']}",
                    "--format=json",
                    "beta",
                    "builds",
                    "triggers",
                    "run",
                    octue_configuration["name"],
                    "--branch=my-branch",
                ],
            )

            # Test waiting for the build trigger run to complete.
            self.assertEqual(
                mock_run.call_args_list[2].args[0],
                [
                    "gcloud",
                    f'--project={octue_configuration["project_name"]}',
                    "--format=json",
                    "builds",
                    "describe",
                    mock_build_id,
                ],
            )

            # Test deploying the streaming Dataflow job.
            options = mock_runner.mock_calls[1].kwargs["options"].get_all_options()

            self.assertFalse(options["update"])
            self.assertTrue(options["streaming"])
            self.assertEqual(options["project"], octue_configuration["project_name"])
            self.assertEqual(options["job_name"], octue_configuration["name"])
            self.assertEqual(options["temp_location"], DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION)
            self.assertEqual(options["region"], octue_configuration["region"])
            self.assertEqual(options["dataflow_service_options"], ["enable_prime"])
            self.assertEqual(options["sdk_container_image"], "my-image")
            self.assertEqual(options["setup_file"], DEFAULT_SETUP_FILE_PATH)

    def test_deploy_with_cloud_build_file_provided(self):
        """Test deploying to Dataflow with a `cloudbuild.yaml` path provided in the `octue.yaml` file"""
        try:
            octue_configuration["cloud_build_configuration_path"] = "cloudbuild.yaml"

            with tempfile.TemporaryDirectory() as temporary_directory:
                octue_configuration_path = self._create_octue_configuration_file(
                    octue_configuration, temporary_directory
                )
                deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

                with patch("subprocess.run", return_value=Mock(returncode=0)) as mock_run:
                    mock_build_id = "my-build-id"

                    with patch(
                        "json.loads",
                        return_value={
                            "metadata": {"build": {"images": ["my-image"], "id": mock_build_id}},
                            "status": "SUCCESS",
                        },
                    ):
                        with patch("octue.cloud.deployment.google.dataflow.pipeline.DataflowRunner") as mock_runner:
                            deployer.deploy()

                # Test the build trigger creation request.
                self.assertEqual(
                    mock_run.call_args_list[0].args[0],
                    EXPECTED_BUILD_TRIGGER_CREATION_COMMAND
                    + [f"--build-config={octue_configuration['cloud_build_configuration_path']}"],
                )

                # Test the build trigger run request.
                self.assertEqual(
                    mock_run.call_args_list[1].args[0],
                    [
                        "gcloud",
                        f"--project={octue_configuration['project_name']}",
                        "--format=json",
                        "beta",
                        "builds",
                        "triggers",
                        "run",
                        octue_configuration["name"],
                        "--branch=my-branch",
                    ],
                )

                # Test waiting for the build trigger run to complete.
                self.assertEqual(
                    mock_run.call_args_list[2].args[0],
                    [
                        "gcloud",
                        f'--project={octue_configuration["project_name"]}',
                        "--format=json",
                        "builds",
                        "describe",
                        mock_build_id,
                    ],
                )

                # Test deploying the streaming Dataflow job.
                options = mock_runner.mock_calls[1].kwargs["options"].get_all_options()

                self.assertTrue(options["streaming"])
                self.assertEqual(options["project"], octue_configuration["project_name"])
                self.assertEqual(options["job_name"], octue_configuration["name"])
                self.assertEqual(options["temp_location"], DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION)
                self.assertEqual(options["region"], octue_configuration["region"])
                self.assertEqual(options["dataflow_service_options"], ["enable_prime"])
                self.assertEqual(options["sdk_container_image"], "my-image")
                self.assertEqual(options["setup_file"], DEFAULT_SETUP_FILE_PATH)

        finally:
            del octue_configuration["cloud_build_configuration_path"]

    def test_deploying_an_update(self):
        """Test deploying an update to a service."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(octue_configuration, temporary_directory)
            deployer = DataflowDeployer(octue_configuration_path, service_id=SERVICE_ID)

            with patch("subprocess.run", return_value=Mock(returncode=0)) as mock_run:
                mock_build_id = "my-build-id"

                with patch(
                    "json.loads",
                    return_value={
                        "metadata": {"build": {"images": ["my-image"], "id": mock_build_id}},
                        "status": "SUCCESS",
                    },
                ):
                    temporary_file = tempfile.NamedTemporaryFile(delete=False)

                    with patch("tempfile.NamedTemporaryFile", return_value=temporary_file):
                        with patch("octue.cloud.deployment.google.dataflow.pipeline.DataflowRunner") as mock_runner:
                            deployer.deploy(update=True)

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
                    f"--project={octue_configuration['project_name']}",
                    "--format=json",
                    "beta",
                    "builds",
                    "triggers",
                    "run",
                    octue_configuration["name"],
                    "--branch=my-branch",
                ],
            )

            # Test waiting for the build trigger run to complete.
            self.assertEqual(
                mock_run.call_args_list[2].args[0],
                [
                    "gcloud",
                    f'--project={octue_configuration["project_name"]}',
                    "--format=json",
                    "builds",
                    "describe",
                    mock_build_id,
                ],
            )

            # Test deploying the streaming Dataflow job.
            options = mock_runner.mock_calls[1].kwargs["options"].get_all_options()

            self.assertTrue(options["update"])
            self.assertTrue(options["streaming"])
            self.assertEqual(options["project"], octue_configuration["project_name"])
            self.assertEqual(options["job_name"], octue_configuration["name"])
            self.assertEqual(options["temp_location"], DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION)
            self.assertEqual(options["region"], octue_configuration["region"])
            self.assertEqual(options["dataflow_service_options"], ["enable_prime"])
            self.assertEqual(options["sdk_container_image"], "my-image")
            self.assertEqual(options["setup_file"], DEFAULT_SETUP_FILE_PATH)
