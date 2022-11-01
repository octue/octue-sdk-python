import copy
import functools
import importlib.util
import json
import logging
import os
import sys

import click
import pkg_resources
from google import auth

from octue.cloud import storage
from octue.cloud.deployment.google.cloud_run.deployer import CloudRunDeployer
from octue.cloud.pub_sub import Subscription, Topic
from octue.cloud.pub_sub.service import Service
from octue.cloud.service_id import convert_service_id_to_pub_sub_form, create_service_sruid, get_service_sruid_parts
from octue.cloud.storage import GoogleCloudStorageClient
from octue.configuration import load_service_and_app_configuration
from octue.definitions import MANIFEST_FILENAME, VALUES_FILENAME
from octue.exceptions import ServiceAlreadyExists
from octue.log_handlers import apply_log_handler, get_remote_handler
from octue.resources import Manifest, service_backends
from octue.runner import Runner
from octue.utils.encoders import OctueJSONEncoder
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
    """The CLI for the Octue SDK. Use it to start an Octue data service or digital twin locally or run an analysis on
    one locally.

    Read more in the docs: https://octue-python-sdk.readthedocs.io/en/latest/
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
    "-c",
    "--service-config",
    type=click.Path(dir_okay=False),
    default="octue.yaml",
    help="The path to an `octue.yaml` file defining the service to run.",
)
@click.option(
    "--input-dir",
    type=click.Path(file_okay=False, exists=True),
    default=".",
    show_default=True,
    help="The path to a directory containing the input values (in a file called 'values.json') and/or input manifest "
    "(in a file called 'manifest.json').",
)
@click.option(
    "-o",
    "--output-file",
    type=click.Path(dir_okay=False),
    default=None,
    show_default=True,
    help="The path to a JSON file to store the output values in, if required.",
)
@click.option(
    "--output-manifest-file",
    type=click.Path(dir_okay=False),
    default=None,
    help="The path to a JSON file to store the output manifest in. The default is 'output_manifest_<analysis_id>.json'.",
)
@click.option(
    "--monitor-messages-file",
    type=click.Path(dir_okay=False),
    default=None,
    show_default=True,
    help="The path to a JSON file in which to store any monitor messages received. Monitor messages will be ignored "
    "if this option isn't provided.",
)
def run(service_config, input_dir, output_file, output_manifest_file, monitor_messages_file):
    """Run an analysis on the given input data using an Octue service or digital twin locally. The output values are
    printed to `stdout`. If an output manifest is produced, it will be saved locally (see the `--output-manifest-file`
    option).
    """
    service_configuration, app_configuration = load_service_and_app_configuration(service_config)

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
        crash_diagnostics_cloud_path=service_configuration.crash_diagnostics_cloud_path,
    )

    if monitor_messages_file:
        if not os.path.exists(os.path.dirname(monitor_messages_file)):
            os.makedirs(os.path.dirname(monitor_messages_file))

        monitor_message_handler = lambda message: _add_monitor_message_to_file(monitor_messages_file, message)

    else:
        monitor_message_handler = None

    analysis = runner.run(
        analysis_id=global_cli_context["analysis_id"],
        input_values=input_values,
        input_manifest=input_manifest,
        analysis_log_level=global_cli_context["log_level"],
        analysis_log_handler=global_cli_context["log_handler"],
        handle_monitor_message=monitor_message_handler,
    )

    click.echo(json.dumps(analysis.output_values, cls=OctueJSONEncoder))

    if analysis.output_values and output_file:
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))

        with open(output_file, "w") as f:
            json.dump(analysis.output_values, f, cls=OctueJSONEncoder, indent=4)

    if analysis.output_manifest:
        if not os.path.exists(os.path.dirname(output_manifest_file)):
            os.makedirs(os.path.dirname(output_manifest_file))

        with open(output_manifest_file or f"output_manifest_{analysis.id}.json", "w") as f:
            json.dump(analysis.output_manifest.to_primitive(), f, cls=OctueJSONEncoder, indent=4)

    return 0


@octue_cli.command()
@click.option(
    "-c",
    "--service-config",
    type=click.Path(dir_okay=False),
    default="octue.yaml",
    help="The path to an `octue.yaml` file defining the service to start.",
)
@click.option(
    "--revision-tag",
    type=str,
    default=None,
    help="A tag to use for this revision of the service (e.g. 1.3.7). This overrides the `OCTUE_SERVICE_REVISION_TAG` "
    "environment variable if it's present. If this option isn't given and the environment variable isn't present, a "
    "random 'cool name' tag is generated e.g 'curious-capybara'.",
)
@click.option(
    "--timeout",
    type=click.INT,
    default=None,
    show_default=True,
    help="A timeout in seconds after which to stop the service. The default is no timeout.",
)
@click.option(
    "--no-rm",
    is_flag=True,
    default=False,
    show_default=True,
    help="Don't delete the Google Pub/Sub topic and subscription for the service on exit.",
)
def start(service_config, revision_tag, timeout, no_rm):
    """Start an Octue service or digital twin locally as a child so it can be asked questions by other Octue services.
    The service's pub/sub topic and subscription are deleted on exit.
    """
    service_revision_tag_override = revision_tag
    service_configuration, app_configuration = load_service_and_app_configuration(service_config)
    service_namespace, service_name, service_revision_tag = get_service_sruid_parts(service_configuration)

    if service_revision_tag_override and service_revision_tag:
        logger.warning(
            "The `OCTUE_SERVICE_REVISION_TAG` environment variable %r has been overridden by the `--revision-tag` CLI "
            "option %r.",
            service_revision_tag,
            service_revision_tag_override,
        )

    service_sruid = create_service_sruid(
        namespace=service_namespace,
        name=service_name,
        revision_tag=service_revision_tag_override or service_revision_tag,
    )

    runner = Runner(
        app_src=service_configuration.app_source_path,
        twine=Twine(source=service_configuration.twine_path),
        configuration_values=app_configuration.configuration_values,
        configuration_manifest=app_configuration.configuration_manifest,
        children=app_configuration.children,
        output_location=app_configuration.output_location,
        crash_diagnostics_cloud_path=service_configuration.crash_diagnostics_cloud_path,
        service_id=service_sruid,
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

    service = Service(service_id=service_sruid, backend=backend, run_function=run_function)

    try:
        service.serve(timeout=timeout, delete_topic_and_subscription_on_exit=not no_rm)

    except ServiceAlreadyExists:
        # Generate and use a new revision tag if the service already exists.
        service_sruid = create_service_sruid(namespace=service_namespace, name=service_name)

        while True:
            user_confirmation = input(
                "Service already exists. Create new service with service revision unique identifier (SRUID) "
                f"{service_sruid!r}? [Y/n]\n"
            )

            if user_confirmation.upper() == "N":
                return

            if user_confirmation.upper() in {"Y", ""}:
                break

        service = Service(service_id=service_sruid, backend=backend, run_function=run_function)
        service.serve(timeout=timeout, delete_topic_and_subscription_on_exit=not no_rm)


@octue_cli.command()
@click.argument(
    "cloud_path",
    type=str,
)
@click.option(
    "--local-path",
    type=click.Path(file_okay=False),
    default=".",
    help="The path to a directory to store the directory of diagnostics data in. Defaults to the current working "
    "directory.",
)
@click.option(
    "--download-datasets",
    is_flag=True,
    help="If provided, download any datasets from the crash diagnostics and update their paths in the configuration and "
    "input manifests to the new local paths.",
)
def get_crash_diagnostics(cloud_path, local_path, download_datasets):
    """Download crash diagnostics for an analysis from the given directory in Google Cloud Storage. The cloud path
    should end in the analysis ID.

    CLOUD_PATH: The path to the directory in Google Cloud Storage containing the diagnostics data.
    """
    analysis_id = storage.path.split(cloud_path)[-1]
    local_path = os.path.join(local_path, analysis_id)

    if download_datasets:
        filter = None
    else:
        filter = lambda blob: any(
            (
                blob.name.endswith(f"configuration_{VALUES_FILENAME}"),
                blob.name.endswith(f"configuration_{MANIFEST_FILENAME}"),
                blob.name.endswith(f"input_{VALUES_FILENAME}"),
                blob.name.endswith(f"input_{MANIFEST_FILENAME}"),
                blob.name.endswith("questions.json"),
            )
        )

    GoogleCloudStorageClient().download_all_files(
        local_path=local_path,
        cloud_path=cloud_path,
        filter=filter,
        recursive=True,
    )

    # Update the manifests with the local paths of the datasets.
    if download_datasets:
        for manifest_type in ("configuration_manifest", "input_manifest"):
            manifest_path = os.path.join(local_path, manifest_type + ".json")

            if not os.path.exists(manifest_path):
                continue

            manifest = Manifest.from_file(manifest_path)

            manifest.update_dataset_paths(
                path_generator=lambda dataset: os.path.join(local_path, f"{manifest_type}_datasets", dataset.name)
            )

            manifest.to_file(manifest_path)

    logger.info("Downloaded crash diagnostics from %r to %r.", cloud_path, local_path)


@octue_cli.group()
def deploy():
    """Deploy a python app to the cloud as an Octue service or digital twin."""


@deploy.command()
@click.option(
    "-c",
    "--service-config",
    type=click.Path(exists=True, dir_okay=False),
    default="octue.yaml",
    show_default=True,
    help="The path to an `octue.yaml` file defining the service to deploy.",
)
@click.option("--no-cache", is_flag=True, help="If provided, don't use the Docker cache.")
@click.option("--update", is_flag=True, help="If provided, allow updates to an existing service.")
def cloud_run(service_config, update, no_cache):
    """Deploy a python app to Google Cloud Run as an Octue service or digital twin."""
    CloudRunDeployer(service_config).deploy(update=update, no_cache=no_cache)


@deploy.command()
@click.option(
    "-c",
    "--service-config",
    type=click.Path(exists=True, dir_okay=False),
    default="octue.yaml",
    show_default=True,
    help="The path to an `octue.yaml` file defining the service to deploy.",
)
@click.option("--no-cache", is_flag=True, help="If provided, don't use the Docker cache when building the image.")
@click.option("--update", is_flag=True, help="If provided, allow updates to an existing service.")
@click.option(
    "--dataflow-job-only",
    is_flag=True,
    help="If provided, skip creating and running the build trigger and just deploy a pre-built image to Dataflow",
)
@click.option("--image-uri", type=str, default=None, help="The actual image URI to use when creating the Dataflow job.")
def dataflow(service_config, no_cache, update, dataflow_job_only, image_uri):
    """Deploy a python app to Google Dataflow as an Octue service or digital twin."""
    if bool(importlib.util.find_spec("apache_beam")):
        # Import the Dataflow deployer only if the `apache-beam` package is available (due to installing `octue` with
        # the `dataflow` extras option).
        from octue.cloud.deployment.google.dataflow.deployer import DataflowDeployer
    else:
        raise ImportWarning(
            "To use this CLI command, you must install `octue` with the `dataflow` option e.g. "
            "`pip install octue[dataflow]`"
        )

    deployer = DataflowDeployer(service_config)

    if dataflow_job_only:
        deployer.create_streaming_dataflow_job(image_uri=image_uri, update=update)
        return

    deployer.deploy(no_cache=no_cache, update=update)


@deploy.command()
@click.argument("project_name")
@click.argument("service_namespace")
@click.argument("service_name")
@click.argument("push_endpoint")
@click.option(
    "--revision-tag",
    is_flag=False,
    default=None,
    show_default=True,
    help="The service revision tag (e.g. 1.0.7). If this option isn't given, a random 'cool name' tag is generated e.g"
    ". 'curious-capybara'.",
)
def create_push_subscription(project_name, service_namespace, service_name, push_endpoint, revision_tag):
    """Create a push subscription on Google Pub/Sub from the Octue service to the push endpoint. If a corresponding
    topic doesn't exist, it will be created. The subscription name is printed on completion.

    PROJECT_NAME is the name of the Google Cloud project in which the subscription will be created

    SERVICE_NAMESPACE is the namespace the service belongs to in kebab case

    SERVICE_NAME is the name of the service in kebab case, unique within its namespace

    PUSH_ENDPOINT is the HTTP/HTTPS endpoint of the service to push to. It should be fully formed and include the
    'https://' prefix
    """
    service_sruid = create_service_sruid(namespace=service_namespace, name=service_name, revision_tag=revision_tag)
    pub_sub_sruid = convert_service_id_to_pub_sub_form(service_sruid)

    topic = Topic(name=pub_sub_sruid, project_name=project_name)
    topic.create(allow_existing=True)

    subscription = Subscription(name=pub_sub_sruid, topic=topic, project_name=project_name, push_endpoint=push_endpoint)
    subscription.create()
    click.echo(service_sruid)


def _add_monitor_message_to_file(path, monitor_message):
    """Add a monitor message to the file at the given path.

    :param str path: the path of the file to add the monitor message to
    :param dict monitor_message: the monitor message to add to the file
    :return None:
    """
    previous_messages = []

    if os.path.exists(path):
        try:
            with open(path) as f:
                previous_messages = json.load(f)
        except json.decoder.JSONDecodeError:
            pass

    previous_messages.append(monitor_message)

    with open(path, "w") as f:
        json.dump(previous_messages, f)


if __name__ == "__main__":
    args = sys.argv[1:] if len(sys.argv) > 1 else []
    octue_cli(args)
