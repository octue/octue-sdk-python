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
                            f"--set-env-vars=SERVICE_ID={service_id}",
                            "--timeout=3600",
                            "--concurrency=80",
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

    def test_create_build_trigger(self):
        """Test that the build trigger is created correctly."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            octue_configuration_path = os.path.join(temporary_directory, "octue.yaml")

            with open(octue_configuration_path, "w") as f:
                yaml.dump(octue_configuration, f)

            deployer = CloudRunDeployer(octue_configuration_path)
            deployer._generate_cloud_build_configuration()

            with patch("subprocess.run", return_value=Mock(returncode=0)) as mock_run:
                deployer._create_build_trigger(deployer.cloud_build_configuration)

            self.assertEqual(
                mock_run.call_args.args[0],
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
                    f"--inline-config={deployer.cloud_build_configuration}",
                    f"--description=Build {octue_configuration['name']} service and deploy it to Cloud Run.",
                    "--branch-pattern=^main$",
                ],
            )
