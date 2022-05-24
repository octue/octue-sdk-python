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
from octue.definitions import MANIFEST_FILENAME, VALUES_FILENAME
from octue.log_handlers import apply_log_handler, get_remote_handler
from octue.resources import service_backends
from octue.runner import Runner
from twined import Twine


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
def octue_cli(id, logger_uri, log_level, force_reset):
    """Octue CLI, enabling a data service / digital twin to be run like a command line application.

    When acting in CLI mode, results are read from and written to disk (see
    https://octue-python-sdk.readthedocs.io/en/latest/ for how to run your application directly without the CLI).
    Once your application has run, you'll be able to find output values and manifest in your specified --output-dir.
    """
    global_cli_context["analysis_id"] = id
    global_cli_context["logger_uri"] = logger_uri
    global_cli_context["log_handler"] = None
    global_cli_context["log_level"] = log_level.upper()
    global_cli_context["force_reset"] = force_reset

    apply_log_handler(log_level=log_level.upper())

    if global_cli_context["logger_uri"]:
        global_cli_context["log_handler"] = get_remote_handler(logger_uri=global_cli_context["logger_uri"])


@octue_cli.command()
@click.option(
    "--service-configuration-path",
    type=click.Path(dir_okay=False),
    default="octue.yaml",
    help="The path to an `octue.yaml` file defining the service to run.",
)
@click.option(
    "--input-dir",
    type=click.Path(),
    default=".",
    show_default=True,
    help="Directory containing input input values and/or manifest.",
)
def run(service_configuration_path, input_dir):
    """Run an analysis on the given input data."""
    service_configuration, app_configuration = load_service_and_app_configuration(service_configuration_path)

    input_values_path = os.path.join(input_dir, VALUES_FILENAME)
    input_manifest_path = os.path.join(input_dir, MANIFEST_FILENAME)

    input_values = None
    input_manifest = None

    if os.path.exists(input_values_path):
        input_values = input_values_path

    if os.path.exists(input_manifest_path):
        input_manifest = input_manifest_path

    runner = Runner(
        app_src=service_configuration.app_source_path,
        twine=Twine(source=service_configuration.twine_path),
        configuration_values=app_configuration.configuration_values,
        configuration_manifest=app_configuration.configuration_manifest,
        children=app_configuration.children,
        output_location=app_configuration.output_location,
    )

    analysis = runner.run(
        analysis_id=global_cli_context["analysis_id"],
        input_values=input_values,
        input_manifest=input_manifest,
        analysis_log_level=global_cli_context["log_level"],
        analysis_log_handler=global_cli_context["log_handler"],
    )

    click.echo(analysis.output_values)
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
    default=None,
    show_default=True,
    help="A unique ID for the service (this should be unique over all time and space). If not provided, the ID is "
    "generated from the `octue.yaml` file.",
)
@click.option("--timeout", type=click.INT, default=None, show_default=True, help="Timeout in seconds for serving.")
@click.option(
    "--rm",
    "--delete-topic-and-subscription-on-exit",
    is_flag=True,
    default=False,
    show_default=True,
    help="Delete the Google Pub/Sub topic and subscription for the service on exit.",
)
def start(service_configuration_path, service_id, timeout, rm):
    """Start the service as a child to be asked questions by other services."""
    service_configuration, app_configuration = load_service_and_app_configuration(service_configuration_path)
    service_id = service_id or service_configuration.service_id

    runner = Runner(
        app_src=service_configuration.app_source_path,
        twine=Twine(source=service_configuration.twine_path),
        configuration_values=app_configuration.configuration_values,
        configuration_manifest=app_configuration.configuration_manifest,
        children=app_configuration.children,
        output_location=app_configuration.output_location,
        service_id=service_id,
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
        name=service_id,
        service_id=service_id,
        backend=backend,
        run_function=run_function,
    )

    service.serve(timeout=timeout, delete_topic_and_subscription_on_exit=rm)


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
    help="An ID to use for the service if a specific one is required (defaults to an automatically generated one).",
)
@click.option("--no-cache", is_flag=True, help="If provided, don't use the Docker cache.")
@click.option("--update", is_flag=True, help="If provided, allow updates to an existing service.")
def cloud_run(octue_configuration_path, service_id, update, no_cache):
    """Deploy an app as a Google Cloud Run service."""
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
    help="An ID to use for the service if a specific one is required (defaults to an automatically generated one).",
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
    if bool(importlib.util.find_spec("apache_beam")):
        # Import the Dataflow deployer only if the `apache-beam` package is available (due to installing `octue` with
        # the `dataflow` extras option).
        from octue.cloud.deployment.google.dataflow.deployer import DataflowDeployer
    else:
        raise ImportWarning(
            "To use this CLI command, you must install `octue` with the `dataflow` option e.g. "
            "`pip install octue[dataflow]`"
        )

    deployer = DataflowDeployer(octue_configuration_path, service_id=service_id)

    if dataflow_job_only:
        deployer.create_streaming_dataflow_job(image_uri=image_uri, update=update)
        return

    deployer.deploy(no_cache=no_cache, update=update)


if __name__ == "__main__":
    args = sys.argv[1:] if len(sys.argv) > 1 else []
    octue_cli(args)
