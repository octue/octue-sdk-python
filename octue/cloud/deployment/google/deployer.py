import subprocess
from google.cloud.devtools.cloudbuild_v1.services.cloud_build import CloudBuildClient


class Deployer:
    def __init__(
        self,
        project_id,
        region,
        build_trigger_name,
        repository_name,
        repository_owner,
        build_configuration_path,
        description=None,
        branch_pattern=None,
        pull_request_pattern=None,
    ):
        self.project_id = project_id
        self.region = region
        self.build_trigger_name = build_trigger_name
        self.repository_name = repository_name
        self.repository_owner = repository_owner
        self.build_configuration_path = build_configuration_path
        self.description = description
        self.branch_pattern = branch_pattern
        self.pull_request_pattern = pull_request_pattern
        self._cloud_build_client = CloudBuildClient()

        if branch_pattern and pull_request_pattern:
            raise ValueError("Only one of `branch_pattern` and `pull_request_pattern` can be provided.")

    def deploy(self):
        self._create_build_trigger()
        self._create_eventarc_run_trigger()
        self._create_cloud_run_service()

    def _create_build_trigger(self):
        if self.branch_pattern:
            pattern_args = [f"--branch-pattern={self.branch_pattern}"]
        else:
            pattern_args = [f"--pull-request-pattern={self.pull_request_pattern}"]

        command = [
            "gcloud",
            "beta",
            "builds",
            "triggers",
            "create",
            "github",
            f"--name={self.build_trigger_name}",
            f"--repo-name={self.repository_name}",
            f"--repo-owner={self.repository_owner}",
            f"--inline-config={self.build_configuration_path}",
            *pattern_args,
        ]

        process = subprocess.run(command, capture_output=True)

        if process.returncode != 0:
            raise subprocess.SubprocessError(process.stderr.decode())

        print(process.stdout.decode() + process.stderr.decode())

    def _create_eventarc_run_trigger(self):
        pass

    def _create_cloud_run_service(self):
        pass
