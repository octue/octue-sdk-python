import os
import tempfile
from unittest.mock import Mock, patch
import yaml

from octue.cloud.deployment.google.deployer import DEFAULT_DOCKERFILE_URL, CloudRunDeployer
from tests.base import BaseTestCase


octue_configuration = {
    "name": "test-service",
    "repository_name": "test-repository",
    "repository_owner": "octue",
    "project_name": "test-project",
    "region": "europe-west2",
    "branch_pattern": "my-branch",
}


class TestCloudRunDeployer(BaseTestCase):
    def test_generate_cloud_build_configuration(self):
        """Test that a correct Google Cloud Build configuration is generated from the given `octue.yaml` file."""
        service_id = "octue.services.0df08f9f-30ad-4db3-8029-ea584b4290b7"
        expected_image_name = f"eu.gcr.io/{octue_configuration['project_name']}/octue/{octue_configuration['repository_name']}/{octue_configuration['name']}"

        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = os.path.join(temporary_directory, "octue.yaml")

            with open(octue_configuration_path, "w") as f:
                yaml.dump(octue_configuration, f)

            deployer = CloudRunDeployer(octue_configuration_path, service_id=service_id)
            deployer._generate_cloud_build_configuration()

            expected_cloud_build_configuration = {
                "steps": [
                    {
                        "id": "Get default Octue Dockerfile",
                        "name": "alpine:latest",
                        "args": ["wget", DEFAULT_DOCKERFILE_URL],
                    },
                    {
                        "id": "Build image",
                        "name": "gcr.io/cloud-builders/docker",
                        "args": [
                            "build",
                            "-t",
                            expected_image_name,
                            ".",
                            "-f",
                            "Dockerfile",
                        ],
                    },
                    {
                        "id": "Push image",
                        "name": "gcr.io/cloud-builders/docker",
                        "args": ["push", expected_image_name],
                    },
                    {
                        "id": "Deploy image to Google Cloud Run",
                        "name": "gcr.io/google.com/cloudsdktool/cloud-sdk:slim",
                        "entrypoint": "gcloud",
                        "args": [
                            "run",
                            "services",
                            "update",
                            "test-service",
                            "--platform=managed",
                            f"--image={expected_image_name}",
                            f"--region={octue_configuration['region']}",
                            "--memory=128Mi",
                            "--cpu=1",
                            f"--set-env-vars=SERVICE_ID={service_id},SERVICE_NAME={octue_configuration['name']}",
                            "--timeout=3600",
                            "--concurrency=10",
                            "--min-instances=0",
                            "--max-instances=10",
                            "--ingress=internal",
                        ],
                    },
                ],
                "images": [expected_image_name],
            }

            # Remove the commit hash from the image name as it will change for each commit made.
            generated_config = deployer.cloud_build_configuration
            generated_config["steps"][1]["args"][2] = generated_config["steps"][1]["args"][2].split(":")[0]
            generated_config["steps"][2]["args"][1] = generated_config["steps"][2]["args"][1].split(":")[0]
            generated_config["steps"][3]["args"][5] = generated_config["steps"][3]["args"][5].split(":")[0]
            generated_config["images"][0] = generated_config["images"][0].split(":")[0]

            self.assertEqual(generated_config, expected_cloud_build_configuration)

    def test_deploy(self):
        """Test that the build trigger creation, build and deployment, and Eventarc run trigger creation are requested
        correctly.
        """
        service_id = "octue.services.4ef88d56-49e0-459b-94e0-d68c5e55e17e"

        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = os.path.join(temporary_directory, "octue.yaml")

            with open(octue_configuration_path, "w") as f:
                yaml.dump(octue_configuration, f)

            deployer = CloudRunDeployer(octue_configuration_path, service_id=service_id)
            deployer._generate_cloud_build_configuration()

            with patch("subprocess.run", return_value=Mock(returncode=0)) as mock_run:
                with patch("octue.cloud.deployment.google.deployer.Topic.create"):
                    with patch(
                        "octue.cloud.deployment.google.deployer.Topic.get_subscriptions",
                        return_value=["test-service"],
                    ):
                        with patch("octue.cloud.deployment.google.deployer.Subscription"):
                            deployer.deploy()

            # Remove the "random" path used for the build configuration in the "--inline-config" argument of the
            # command.
            build_trigger_command_without_inline_config_path = mock_run.call_args_list[0].args[0]
            build_trigger_command_without_inline_config_path.pop(9)

            # Test the build trigger creation request.
            self.assertEqual(
                build_trigger_command_without_inline_config_path,
                [
                    "gcloud",
                    "beta",
                    "builds",
                    "triggers",
                    "create",
                    "github",
                    f"--name={octue_configuration['name']}",
                    f"--repo-name={octue_configuration['repository_name']}",
                    f"--repo-owner={octue_configuration['repository_owner']}",
                    f"--description=Build the {octue_configuration['name']!r} service and deploy it to Cloud Run.",
                    f"--branch-pattern={octue_configuration['branch_pattern']}",
                ],
            )

            # Test the build request.
            self.assertEqual(mock_run.call_args_list[1].args[0][:4], ["gcloud", "builds", "submit", "."])

            # Test setting the Cloud Run service to accept unauthenticated requests.
            self.assertEqual(
                mock_run.call_args_list[2].args[0],
                [
                    "gcloud",
                    "run",
                    "services",
                    "add-iam-policy-binding",
                    octue_configuration["name"],
                    f'--region={octue_configuration["region"]}',
                    "--member=allUsers",
                    "--role=roles/run.invoker",
                ],
            )

            # Test the Eventarc run trigger creation request.
            self.assertEqual(
                mock_run.call_args_list[3].args[0],
                [
                    "gcloud",
                    "beta",
                    "eventarc",
                    "triggers",
                    "create",
                    f'{octue_configuration["name"]}-trigger',
                    "--matching-criteria=type=google.cloud.pubsub.topic.v1.messagePublished",
                    "--destination-run-service=test-service",
                    "--location=europe-west2",
                    f"--transport-topic={service_id}",
                ],
            )
