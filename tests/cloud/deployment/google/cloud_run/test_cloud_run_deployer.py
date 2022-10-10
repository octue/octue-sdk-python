import copy
import os
import tempfile
from unittest.mock import Mock, patch

from octue.cloud.deployment.google.cloud_run.deployer import DEFAULT_CLOUD_RUN_DOCKERFILE_URL, CloudRunDeployer
from octue.cloud.emulators._pub_sub import MockPublisher
from octue.exceptions import DeploymentError
from octue.utils.patches import MultiPatcher
from tests.base import BaseTestCase


OCTUE_CONFIGURATION = {
    "services": [
        {
            "name": "test-service",
            "namespace": "testing",
            "repository_name": "test-repository",
            "repository_owner": "octue",
            "project_name": "test-project",
            "region": "europe-west2",
            "branch_pattern": "my-branch",
        }
    ]
}

SERVICE = OCTUE_CONFIGURATION["services"][0]
OCTUE_SERVICE_TAG = "my-tag"
SRUID = f"{SERVICE['namespace']}/{SERVICE['name']}:{OCTUE_SERVICE_TAG}"
PUB_SUB_SRUID = f"{SERVICE['namespace']}.{SERVICE['name']}.{OCTUE_SERVICE_TAG}"

GET_SUBSCRIPTIONS_METHOD_PATH = "octue.cloud.deployment.google.cloud_run.deployer.Topic.get_subscriptions"
EXPECTED_IMAGE_NAME = f"eu.gcr.io/{SERVICE['project_name']}/{SERVICE['repository_name']}/{SRUID}:$SHORT_SHA"

EXPECTED_CLOUD_BUILD_CONFIGURATION = {
    "steps": [
        {
            "id": "Get default Octue Dockerfile",
            "name": "alpine:latest",
            "args": ["wget", DEFAULT_CLOUD_RUN_DOCKERFILE_URL],
        },
        {
            "id": "Build image",
            "name": "gcr.io/cloud-builders/docker",
            "args": [
                "-c",
                f"docker build '-t' {EXPECTED_IMAGE_NAME!r} . '-f' Dockerfile",
            ],
            "entrypoint": "bash",
        },
        {
            "id": "Push image",
            "name": "gcr.io/cloud-builders/docker",
            "args": ["push", EXPECTED_IMAGE_NAME],
        },
        {
            "id": "Deploy image to Google Cloud Run",
            "name": "gcr.io/google.com/cloudsdktool/cloud-sdk:slim",
            "entrypoint": "gcloud",
            "args": [
                "run",
                "services",
                "update",
                f"{SRUID}",
                "--platform=managed",
                f"--image={EXPECTED_IMAGE_NAME}",
                f"--region={SERVICE['region']}",
                "--memory=128Mi",
                "--cpu=1",
                f"--set-env-vars=OCTUE_SERVICE_NAMESPACE={SERVICE['namespace']},OCTUE_SERVICE_NAME={SERVICE['name']},OCTUE_SERVICE_TAG={OCTUE_SERVICE_TAG}",
                "--timeout=3600",
                "--concurrency=10",
                "--min-instances=0",
                "--max-instances=10",
                "--ingress=internal",
            ],
        },
    ],
    "images": [EXPECTED_IMAGE_NAME],
}

EXPECTED_BUILD_TRIGGER_CREATION_COMMAND = [
    "gcloud",
    f"--project={SERVICE['project_name']}",
    "beta",
    "builds",
    "triggers",
    "create",
    "github",
    f"--name={PUB_SUB_SRUID}",
    f"--repo-name={SERVICE['repository_name']}",
    f"--repo-owner={SERVICE['repository_owner']}",
    f"--description=Build the '{SRUID}' service and deploy it to Cloud Run.",
    f"--branch-pattern={SERVICE['branch_pattern']}",
]


class TestCloudRunDeployer(BaseTestCase):
    def test_generate_cloud_build_configuration(self):
        """Test that a correct Google Cloud Build configuration is generated from the given `octue.yaml` file."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(OCTUE_CONFIGURATION, temporary_directory)

            with patch.dict(os.environ, {"OCTUE_SERVICE_TAG": OCTUE_SERVICE_TAG}):
                deployer = CloudRunDeployer(octue_configuration_path)

            deployer._generate_cloud_build_configuration()

        self.assertEqual(deployer.generated_cloud_build_configuration, EXPECTED_CLOUD_BUILD_CONFIGURATION)

    def test_generate_cloud_build_configuration_with_custom_dockerfile(self):
        """Test that a correct Google Cloud Build configuration is generated from the given `octue.yaml` file when a
        dockerfile path is given.
        """
        octue_configuration_with_custom_dockerfile = copy.deepcopy(OCTUE_CONFIGURATION)
        octue_configuration_with_custom_dockerfile["services"][0]["dockerfile_path"] = "path/to/Dockerfile"

        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(
                octue_configuration_with_custom_dockerfile,
                temporary_directory,
            )

            with patch.dict(os.environ, {"OCTUE_SERVICE_TAG": OCTUE_SERVICE_TAG}):
                deployer = CloudRunDeployer(octue_configuration_path)

            deployer._generate_cloud_build_configuration()

        # Expect the extra "Get default Octue Dockerfile" step to be absent and the given Dockerfile path to be
        # provided in the first step.
        expected_cloud_build_configuration = copy.deepcopy(EXPECTED_CLOUD_BUILD_CONFIGURATION)
        expected_cloud_build_configuration["steps"] = expected_cloud_build_configuration["steps"][1:]

        expected_cloud_build_configuration["steps"][0]["args"][1] = (
            f"docker build '-t' '{EXPECTED_IMAGE_NAME}' . '-f' "
            f"{octue_configuration_with_custom_dockerfile['services'][0]['dockerfile_path']}"
        )

        self.assertEqual(deployer.generated_cloud_build_configuration, expected_cloud_build_configuration)

    def test_generate_cloud_build_configuration_with_build_secrets(self):
        """Test generating a Cloud Build configuration with build secrets."""
        secret_name = "MY_BIG_SECRET"

        octue_configuration_with_build_secrets = copy.deepcopy(OCTUE_CONFIGURATION)
        service = octue_configuration_with_build_secrets["services"][0]
        service["secrets"] = {"build": [secret_name]}

        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(
                octue_configuration_with_build_secrets,
                temporary_directory,
            )

            with patch.dict(os.environ, {"OCTUE_SERVICE_TAG": OCTUE_SERVICE_TAG}):
                deployer = CloudRunDeployer(octue_configuration_path)

            deployer._generate_cloud_build_configuration()

        expected_cloud_build_configuration = copy.deepcopy(EXPECTED_CLOUD_BUILD_CONFIGURATION)

        expected_cloud_build_configuration["availableSecrets"] = {
            "secretManager": [
                {
                    "versionName": f'projects/{service["project_name"]}/secrets/{secret_name}/versions/latest',
                    "env": secret_name,
                }
            ]
        }

        expected_cloud_build_configuration["steps"][1]["args"][
            1
        ] = f"docker build '-t' {EXPECTED_IMAGE_NAME!r} --build-arg={secret_name}=$${secret_name} . '-f' Dockerfile"

        expected_cloud_build_configuration["steps"][1]["secretEnv"] = [secret_name]

        self.assertEqual(deployer.generated_cloud_build_configuration, expected_cloud_build_configuration)

    def test_deploy(self):
        """Test that the build trigger creation, build and deployment, and Eventarc run trigger creation are requested
        correctly.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(OCTUE_CONFIGURATION, temporary_directory)

            with patch.dict(os.environ, {"OCTUE_SERVICE_TAG": OCTUE_SERVICE_TAG}):
                deployer = CloudRunDeployer(octue_configuration_path)

            mock_build_id = "my-build-id"

            with patch("subprocess.run", return_value=Mock(returncode=0)) as mock_run:
                with MultiPatcher(
                    patches=[
                        patch("octue.cloud.deployment.google.cloud_run.deployer.Topic.create"),
                        patch(GET_SUBSCRIPTIONS_METHOD_PATH, return_value=[f"{PUB_SUB_SRUID}"]),
                        patch("octue.cloud.deployment.google.cloud_run.deployer.Subscription"),
                        patch("octue.cloud.pub_sub.topic.PublisherClient", MockPublisher),
                        patch(
                            "json.loads",
                            return_value={
                                "metadata": {"build": {"images": ["my-image"], "id": mock_build_id}},
                                "status": "SUCCESS",
                            },
                        ),
                    ]
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
                    f"--project={SERVICE['project_name']}",
                    "--format=json",
                    "beta",
                    "builds",
                    "triggers",
                    "run",
                    f"{PUB_SUB_SRUID}",
                    "--branch=my-branch",
                ],
            )

            # Test waiting for the build trigger run to complete.
            self.assertEqual(
                mock_run.call_args_list[2].args[0],
                [
                    "gcloud",
                    f'--project={SERVICE["project_name"]}',
                    "--format=json",
                    "builds",
                    "describe",
                    mock_build_id,
                ],
            )

            # Test setting the Cloud Run service to accept unauthenticated requests.
            self.assertEqual(
                mock_run.call_args_list[3].args[0],
                [
                    "gcloud",
                    f'--project={SERVICE["project_name"]}',
                    "run",
                    "services",
                    "add-iam-policy-binding",
                    f"{SRUID}",
                    f'--region={SERVICE["region"]}',
                    "--member=allUsers",
                    "--role=roles/run.invoker",
                ],
            )

            # Test the Eventarc run trigger creation request.
            self.assertEqual(
                mock_run.call_args_list[4].args[0],
                [
                    "gcloud",
                    f'--project={SERVICE["project_name"]}',
                    "beta",
                    "eventarc",
                    "triggers",
                    "create",
                    f"{SRUID}-trigger",
                    "--matching-criteria=type=google.cloud.pubsub.topic.v1.messagePublished",
                    f"--destination-run-service={SRUID}",
                    f"--location={SERVICE['region']}",
                    f"--transport-topic=octue.services.{PUB_SUB_SRUID}",
                ],
            )

    def test_create_build_trigger_with_update(self):
        """Test that creating a build trigger for a service when one already exists and the deployer is in `update`
        mode results in the existing trigger being deleted and recreated.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(OCTUE_CONFIGURATION, temporary_directory)

            with patch.dict(os.environ, {"OCTUE_SERVICE_TAG": OCTUE_SERVICE_TAG}):
                deployer = CloudRunDeployer(octue_configuration_path)

            deployer._generate_cloud_build_configuration()

            temporary_file = tempfile.NamedTemporaryFile(delete=False)

            with patch("tempfile.NamedTemporaryFile", return_value=temporary_file):
                with patch(
                    "octue.cloud.deployment.google.cloud_run.deployer.CloudRunDeployer._run_command",
                    side_effect=[DeploymentError("already exists"), None, None],
                ) as mock_run_command:
                    with patch("builtins.print") as mock_print:
                        deployer._create_build_trigger(update=True)

        self.assertEqual(mock_print.call_args[0][0], "recreated.")

        expected_build_trigger_creation_command = EXPECTED_BUILD_TRIGGER_CREATION_COMMAND + [
            f"--inline-config={temporary_file.name}"
        ]

        # Test the build trigger creation request.
        self.assertEqual(mock_run_command.call_args_list[0].args[0], expected_build_trigger_creation_command)

        # Test that trigger deletion is requested.
        self.assertEqual(
            mock_run_command.call_args_list[1].args[0],
            [
                "gcloud",
                f"--project={SERVICE['project_name']}",
                "beta",
                "builds",
                "triggers",
                "delete",
                f"{PUB_SUB_SRUID}",
            ],
        )

        # Test the build trigger creation request is retried.
        self.assertEqual(mock_run_command.call_args_list[2].args[0], expected_build_trigger_creation_command)

    def test_create_eventarc_run_trigger_with_update(self):
        """Test that creating an Eventarc run trigger for a service when one already exists and the deployer is in
        `update` mode results in the Eventarc subscription update being skipped and an "already exists" message being
        printed.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = self._create_octue_configuration_file(OCTUE_CONFIGURATION, temporary_directory)
            deployer = CloudRunDeployer(octue_configuration_path)

            with patch("octue.cloud.deployment.google.cloud_run.deployer.Topic.create"):
                with patch(
                    "octue.cloud.deployment.google.cloud_run.deployer.CloudRunDeployer._run_command",
                    side_effect=DeploymentError("already exists"),
                ):
                    with patch(GET_SUBSCRIPTIONS_METHOD_PATH) as mock_get_subscriptions:
                        with patch("builtins.print") as mock_print:
                            deployer._create_eventarc_run_trigger(update=True)

        mock_get_subscriptions.assert_not_called()
        self.assertEqual(mock_print.call_args[0][0], "already exists.")
