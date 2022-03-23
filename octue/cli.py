import copy
import functools
import importlib.util
import logging
import os
import sys

import click
import pkg_resources
from google import auth

from octue.cloud.deployment.google.cloud_run.deployer import CloudRunDeployer
from octue.cloud.pub_sub.service import Service
from octue.configuration import load_service_and_app_configuration
from octue.definitions import CHILDREN_FILENAME, FOLDER_DEFAULTS, MANIFEST_FILENAME, VALUES_FILENAME
from octue.exceptions import DeploymentError
from octue.log_handlers import apply_log_handler, get_remote_handler
from octue.resources import service_backends
from octue.runner import Runner
from twined import Twine


# Import the Dataflow deployer only if the `apache-beam` package is available (due to installing `octue` with the
# `dataflow` extras option).
APACHE_BEAM_PACKAGE_AVAILABLE = bool(importlib.util.find_spec("apache_beam"))

if APACHE_BEAM_PACKAGE_AVAILABLE:
    from octue.cloud.deployment.google.dataflow.deployer import DataflowDeployer

logger = logging.getLogger(__name__)

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
    help="Skips the input checking. This can be a timesaver if you already checked data directories (especially if manifests are large).",
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

    apply_log_handler(log_level=log_level.upper())

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
    "--service-configuration-path",
    type=click.Path(dir_okay=False),
    default="octue.yaml",
    help="The path to an `octue.yaml` file defining the service to start.",
)
@click.option(
    "--service-id",
    type=click.STRING,
    help="The unique ID of the server (this should be unique over all time and space).",
)
@click.option("--timeout", type=click.INT, default=None, show_default=True, help="Timeout in seconds for serving.")
@click.option(
    "--delete-topic-and-subscription-on-exit",
    is_flag=True,
    default=False,
    show_default=True,
    help="Delete Google Pub/Sub topics and subscriptions on exit.",
)
def start(service_configuration_path, service_id, timeout, delete_topic_and_subscription_on_exit):
    """Start the service as a child to be asked questions by other services."""
    service_configuration, app_configuration = load_service_and_app_configuration(service_configuration_path)

    runner = Runner(
        app_src=service_configuration.app_source_path,
        twine=Twine(source=service_configuration.twine_path),
        configuration_values=app_configuration.configuration_values,
        configuration_manifest=app_configuration.configuration_manifest,
        children=app_configuration.children,
        skip_checks=global_cli_context["skip_checks"],
    )

    run_function = functools.partial(
        runner.run,
        analysis_log_level=global_cli_context["log_level"],
        analysis_log_handler=global_cli_context["log_handler"],
    )

    backend_configuration_values = (app_configuration.configuration_values or {}).get("backend")

    if backend_configuration_values:
        backend_configuration_values = copy.deepcopy(backend_configuration_values)
        backend = service_backends.get_backend(backend_configuration_values.pop("name"))(**backend_configuration_values)
    else:
        # If no backend details are provided, use Google Pub/Sub with the default project.
        _, project_name = auth.default()
        backend = service_backends.get_backend()(project_name=project_name)

    service = Service(
        name=service_configuration.name,
        service_id=service_id,
        backend=backend,
        run_function=run_function,
    )

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
@click.option("--no-cache", is_flag=True, help="If provided, don't use the Docker cache when building the image.")
@click.option("--update", is_flag=True, help="If provided, allow updates to an existing service.")
@click.option(
    "--dataflow-job-only",
    is_flag=True,
    help="If provided, skip creating and running the build trigger and just deploy a pre-built image to Dataflow",
)
@click.option("--image-uri", type=str, default=None, help="The actual image URI to use when creating the Dataflow job.")
def dataflow(octue_configuration_path, service_id, no_cache, update, dataflow_job_only, image_uri):
    """Deploy an app as a Google Dataflow streaming job."""
    if not APACHE_BEAM_PACKAGE_AVAILABLE:
        raise ImportWarning(
            "To use this CLI command, you must install `octue` with the `dataflow` option e.g. "
            "`pip install octue[dataflow]`"
        )

    if update and not service_id:
        raise DeploymentError("If updating a service, you must also provide the `--service-id` argument.")

    deployer = DataflowDeployer(octue_configuration_path, service_id=service_id)

    if dataflow_job_only:
        deployer.create_streaming_dataflow_job(image_uri=image_uri, update=update)
        return

    deployer.deploy(no_cache=no_cache, update=update)


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
