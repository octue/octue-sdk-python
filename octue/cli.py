import functools
import os
import sys

import click
import pkg_resources

from octue.cloud.deployment.google.dataflow.deploy import (
    DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION,
    DEFAULT_IMAGE_URI,
    DEFAULT_SETUP_FILE_PATH,
    deploy_streaming_pipeline,
)
from octue.cloud.deployment.google.deployer import CloudRunDeployer
from octue.cloud.pub_sub.service import Service
from octue.definitions import CHILDREN_FILENAME, FOLDER_DEFAULTS, MANIFEST_FILENAME, VALUES_FILENAME
from octue.exceptions import DeploymentError
from octue.log_handlers import get_remote_handler
from octue.resources import service_backends
from octue.runner import Runner
from twined import Twine


global_cli_context = {}


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--id",
    default=None,
    type=click.UUID,
    show_default=True,
    help="UUID of the analysis being undertaken. None (for local use) will cause a unique ID to be generated.",
)
@click.option(
    "--skip-checks/--no-skip-checks",
    default=False,
    is_flag=True,
    show_default=True,
    help="Skips the input checking. This can be a timesaver if you already checked "
    "data directories (especially if manifests are large).",
)
@click.option("--logger-uri", default=None, show_default=True, help="Stream logs to a websocket at the given URI.")
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error"], case_sensitive=False),
    show_default=True,
    help="Log level used for the analysis.",
)
@click.option(
    "--force-reset/--no-force-reset",
    default=True,
    is_flag=True,
    show_default=True,
    help="Forces a reset of analysis cache and outputs [For future use, currently not implemented]",
)
@click.version_option(version=pkg_resources.get_distribution("octue").version)
def octue_cli(id, skip_checks, logger_uri, log_level, force_reset):
    """Octue CLI, enabling a data service / digital twin to be run like a command line application.

    When acting in CLI mode, results are read from and written to disk (see
    https://octue-python-sdk.readthedocs.io/en/latest/ for how to run your application directly without the CLI).
    Once your application has run, you'll be able to find output values and manifest in your specified --output-dir.
    """
    global_cli_context["analysis_id"] = id
    global_cli_context["skip_checks"] = skip_checks
    global_cli_context["logger_uri"] = logger_uri
    global_cli_context["log_handler"] = None
    global_cli_context["log_level"] = log_level.upper()
    global_cli_context["force_reset"] = force_reset

    if global_cli_context["logger_uri"]:
        global_cli_context["log_handler"] = get_remote_handler(logger_uri=global_cli_context["logger_uri"])


@octue_cli.command()
@click.option(
    "--app-dir",
    type=click.Path(),
    default=".",
    show_default=True,
    help="Directory containing your source code (app.py)",
)
@click.option(
    "--data-dir",
    type=click.Path(),
    default=".",
    show_default=True,
    help="Location of directories containing configuration values and manifest, input values and manifest, and output "
    "directory.",
)
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    show_default=True,
    help="Directory containing configuration (overrides --data-dir).",
)
@click.option(
    "--input-dir",
    type=click.Path(),
    default=None,
    show_default=True,
    help="Directory containing input (overrides --data-dir).",
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    show_default=True,
    help="Directory to write outputs as files (overrides --data-dir).",
)
@click.option("--twine", type=click.Path(), default="twine.json", show_default=True, help="Location of Twine file.")
def run(app_dir, data_dir, config_dir, input_dir, output_dir, twine):
    """Run an analysis on the given input data."""
    config_dir = config_dir or os.path.join(data_dir, FOLDER_DEFAULTS["configuration"])
    input_dir = input_dir or os.path.join(data_dir, FOLDER_DEFAULTS["input"])
    output_dir = output_dir or os.path.join(data_dir, FOLDER_DEFAULTS["output"])

    twine = Twine(source=twine)

    (
        configuration_values,
        configuration_manifest,
        input_values,
        input_manifest,
        children,
    ) = set_unavailable_strand_paths_to_none(
        twine,
        (
            ("configuration_values", os.path.join(config_dir, VALUES_FILENAME)),
            ("configuration_manifest", os.path.join(config_dir, MANIFEST_FILENAME)),
            ("input_values", os.path.join(input_dir, VALUES_FILENAME)),
            ("input_manifest", os.path.join(input_dir, MANIFEST_FILENAME)),
            ("children", os.path.join(config_dir, CHILDREN_FILENAME)),
        ),
    )

    runner = Runner(
        app_src=app_dir,
        twine=twine,
        configuration_values=configuration_values,
        configuration_manifest=configuration_manifest,
        output_manifest_path=os.path.join(output_dir, MANIFEST_FILENAME),
        children=children,
        skip_checks=global_cli_context["skip_checks"],
    )

    analysis = runner.run(
        analysis_id=global_cli_context["analysis_id"],
        input_values=input_values,
        input_manifest=input_manifest,
        analysis_log_level=global_cli_context["log_level"],
        analysis_log_handler=global_cli_context["log_handler"],
    )

    analysis.finalise(output_dir=output_dir)
    return 0


@octue_cli.command()
@click.option(
    "--app-dir",
    type=click.Path(),
    default=".",
    show_default=True,
    help="Directory containing your source code (app.py)",
)
@click.option(
    "--data-dir",
    type=click.Path(),
    default=".",
    show_default=True,
    help="Location of directories containing configuration values and manifest.",
)
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    show_default=True,
    help="Directory containing configuration (overrides --data-dir).",
)
@click.option(
    "--service-id",
    type=click.STRING,
    help="The unique ID of the server (this should be unique over all time and space).",
)
@click.option("--twine", type=click.Path(), default="twine.json", show_default=True, help="Location of Twine file.")
@click.option("--timeout", type=click.INT, default=None, show_default=True, help="Timeout in seconds for serving.")
@click.option(
    "--delete-topic-and-subscription-on-exit",
    is_flag=True,
    default=False,
    show_default=True,
    help="Delete Google Pub/Sub topics and subscriptions on exit.",
)
def start(app_dir, data_dir, config_dir, service_id, twine, timeout, delete_topic_and_subscription_on_exit):
    """Start the service as a server to be asked questions by other services."""
    config_dir = config_dir or os.path.join(data_dir, FOLDER_DEFAULTS["configuration"])
    twine = Twine(source=twine)

    configuration_values, configuration_manifest, children = set_unavailable_strand_paths_to_none(
        twine,
        (
            ("configuration_values", os.path.join(config_dir, VALUES_FILENAME)),
            ("configuration_manifest", os.path.join(config_dir, MANIFEST_FILENAME)),
            ("children", os.path.join(config_dir, CHILDREN_FILENAME)),
        ),
    )

    runner = Runner(
        app_src=app_dir,
        twine=twine,
        configuration_values=configuration_values,
        configuration_manifest=configuration_manifest,
        children=children,
        skip_checks=global_cli_context["skip_checks"],
    )

    run_function = functools.partial(
        runner.run,
        analysis_log_level=global_cli_context["log_level"],
        analysis_log_handler=global_cli_context["log_handler"],
    )

    backend_configuration_values = runner.configuration["configuration_values"]["backend"]
    backend = service_backends.get_backend(backend_configuration_values.pop("name"))(**backend_configuration_values)

    service = Service(service_id=service_id, backend=backend, run_function=run_function)
    service.serve(timeout=timeout, delete_topic_and_subscription_on_exit=delete_topic_and_subscription_on_exit)


@octue_cli.group()
def deploy():
    """Deploy an app to the cloud as a service."""


@deploy.command()
@click.option(
    "--octue-configuration-path",
    type=click.Path(exists=True, dir_okay=False),
    default="octue.yaml",
    show_default=True,
    help="Path to an octue.yaml file.",
)
@click.option(
    "--service-id",
    type=str,
    default=None,
    help="A UUID to use for the service if a specific one is required (defaults to an automatically generated one).",
)
@click.option("--no-cache", is_flag=True, help="If provided, don't use the Docker cache.")
@click.option("--update", is_flag=True, help="If provided, allow updates to an existing service.")
def cloud_run(octue_configuration_path, service_id, update, no_cache):
    """Deploy an app as a Google Cloud Run service."""
    if update and not service_id:
        raise DeploymentError("If updating a service, you must also provide the `--service-id` argument.")

    CloudRunDeployer(octue_configuration_path, service_id=service_id).deploy(update=update, no_cache=no_cache)


@deploy.command()
@click.argument("service_name", type=str)
@click.argument("service_id", type=str)
@click.argument("project_name", type=str)
@click.argument("region", type=str)
@click.option(
    "--runner",
    type=str,
    default="DataflowRunner",
    show_default=True,
    help="One of the valid apache-beam runners to use to execute the pipeline.",
)
@click.option(
    "--setup-file-path",
    type=click.Path(exists=True, dir_okay=False),
    default=DEFAULT_SETUP_FILE_PATH,
    show_default=True,
    help="The path to a python `setup.py` file to use for the service.",
)
@click.option(
    "--image-uri",
    type=str,
    default=DEFAULT_IMAGE_URI,
    show_default=True,
    help="The URI of the apache-beam-based Docker image to use for the service.",
)
@click.option(
    "--temporary-files-location",
    type=str,
    default=DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION,
    show_default=True,
    help="The Google Cloud Storage path to save temporary files from the service at.",
)
def dataflow(
    service_name,
    service_id,
    project_name,
    region,
    runner,
    setup_file_path,
    image_uri,
    temporary_files_location,
):
    """Deploy an app as a Google Dataflow streaming service.

    SERVICE_NAME - the name to give the service

    SERVICE_ID - the ID that the service can be reached at by other services e.g. "octue.services.06fad9a3-fe6b-44c5-a239-5dd2a49cdd4e"

    PROJECT_NAME - the name of the project to deploy to

    REGION - the cloud region to deploy in
    """
    deploy_streaming_pipeline(
        service_name=service_name,
        project_name=project_name,
        service_id=service_id,
        region=region,
        runner=runner,
        setup_file_path=os.path.abspath(setup_file_path),
        image_uri=image_uri,
        temporary_files_location=temporary_files_location,
    )


def set_unavailable_strand_paths_to_none(twine, strands):
    """Set paths to unavailable strands to None, leaving the paths of available strands as they are."""
    updated_strand_paths = []

    for strand_name, strand in strands:
        if strand_name not in twine.available_strands:
            updated_strand_paths.append(None)
        else:
            updated_strand_paths.append(strand)

    return updated_strand_paths


if __name__ == "__main__":
    args = sys.argv[1:] if len(sys.argv) > 1 else []
    octue_cli(args)
