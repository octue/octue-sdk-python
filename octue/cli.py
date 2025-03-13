import copy
import functools
import json
import logging
import os
import sys

import click
from google import auth

from octue.cloud import storage
from octue.cloud.events.answer_question import answer_question
from octue.cloud.events.replayer import EventReplayer
from octue.cloud.events.question import make_question_event
from octue.cloud.events.validation import VALID_EVENT_KINDS
from octue.cloud.pub_sub.bigquery import get_events, DEFAULT_EVENT_STORE_TABLE_ID
from octue.cloud.pub_sub.service import Service
from octue.cloud.service_id import create_sruid, get_sruid_parts
from octue.cloud.storage import GoogleCloudStorageClient
from octue.configuration import ServiceConfiguration, load_service_and_app_configuration
from octue.definitions import LOCAL_SDK_VERSION, MANIFEST_FILENAME, VALUES_FILENAME
from octue.exceptions import ServiceAlreadyExists
from octue.log_handlers import apply_log_handler, get_remote_handler
from octue.resources import Child, Manifest, service_backends
from octue.runner import Runner
from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder

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
@click.version_option(version=LOCAL_SDK_VERSION)
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


@octue_cli.group()
def question():
    """Ask a new question to an Octue Twined data service or interact with a previous question."""


@question.group()
def ask():
    """Ask a new question to an Octue Twined data service."""


@ask.command()
@click.argument("sruid", type=str)
@click.option(
    "-i",
    "--input-values",
    type=str,
    default=None,
    help="Any input values for the question as a JSON-encoded string.",
)
@click.option(
    "-m",
    "--input-manifest",
    type=str,
    default=None,
    help="An optional input manifest for the question serialised as a JSON-encoded string.",
)
@click.option(
    "-p",
    "--project-name",
    type=str,
    default=None,
    help="The name of the Google Cloud project the service is deployed in. If not provided, the project name is "
    "detected from the local Google application credentials if present.",
)
@click.option(
    "--asynchronous",
    is_flag=True,
    help="If provided, ask the question and detach. The result and other events can be retrieved from the event store "
    "later.",
)
@click.option(
    "-c",
    "--service-config",
    type=click.Path(dir_okay=False),
    default=None,
    help="An optional path to an `octue.yaml` file defining service registries to use. If not provided, the "
    "`OCTUE_SERVICE_CONFIGURATION_PATH` environment variable is used if present, otherwise the local path `octue.yaml` "
    "is used.",
)
def remote(sruid, input_values, input_manifest, project_name, asynchronous, service_config):
    """Ask a question to a remote Octue Twined service.

    SRUID should be a valid service revision unique identifier for an existing Octue Twined service e.g.

        octue question ask remote your-org/example-service:1.2.0
    """
    service_configuration = ServiceConfiguration.from_file(service_config, allow_not_found=True)

    if service_configuration:
        service_registries = service_configuration.service_registries
    else:
        service_registries = None

    if input_values:
        input_values = json.loads(input_values, cls=OctueJSONDecoder)

    if input_manifest:
        input_manifest = Manifest.deserialise(input_manifest, from_string=True)

    if not project_name:
        _, project_name = auth.default()

    child = Child(
        id=sruid,
        backend={"name": "GCPPubSubBackend", "project_name": project_name},
        service_registries=service_registries,
    )

    answer, question_uuid = child.ask(
        input_values=input_values,
        input_manifest=input_manifest,
        asynchronous=asynchronous,
    )

    if asynchronous:
        click.echo(question_uuid)
        return

    output_manifest = answer.get("output_manifest")

    if output_manifest:
        answer["output_manifest"] = output_manifest.to_primitive()

    click.echo(json.dumps(answer, cls=OctueJSONEncoder))


@ask.command()
@click.option(
    "-i",
    "--input-values",
    type=str,
    default=None,
    help="Any input values for the question, serialised as a JSON-encoded string.",
)
@click.option(
    "-m",
    "--input-manifest",
    type=str,
    default=None,
    help="An optional input manifest for the question, serialised as a JSON-encoded string.",
)
@click.option(
    "-a",
    "--attributes",
    type=str,
    default=None,
    help="An optional full set of event attributes for the question, serialised as a JSON-encoded string. If not "
    "provided, the question will be an originator question.",
)
@click.option(
    "-c",
    "--service-config",
    type=click.Path(dir_okay=False),
    default=None,
    help="The path to an `octue.yaml` file defining the service to run. If not provided, the "
    "`OCTUE_SERVICE_CONFIGURATION_PATH` environment variable is used if present, otherwise the local path `octue.yaml` "
    "is used.",
)
def local(input_values, input_manifest, attributes, service_config):
    """Ask a question to a local Octue Twined service.

    This command is similar to running `octue start` and asking the resulting local service revision a question
    via Pub/Sub. Instead of starting a local Pub/Sub service revision, however, no Pub/Sub subscription or subscriber is
    created; the question is instead passed directly to local the service revision without Pub/Sub being involved.
    Everything after this runs the same, though, with the service revision emitting any events via Pub/Sub as usual.
    """
    if input_values:
        input_values = json.loads(input_values, cls=OctueJSONDecoder)

    if input_manifest:
        input_manifest = json.loads(input_manifest, cls=OctueJSONDecoder)

    service_configuration, app_configuration = load_service_and_app_configuration(service_config)

    if attributes:
        attributes = json.loads(attributes, cls=OctueJSONDecoder)
        question = make_question_event(input_values=input_values, input_manifest=input_manifest, attributes=attributes)
    else:
        namespace, name, revision_tag = get_sruid_parts(service_configuration)
        recipient = create_sruid(namespace=namespace, name=name, revision_tag=revision_tag)

        question = make_question_event(
            input_values=input_values,
            input_manifest=input_manifest,
            sender=create_sruid(),
            recipient=recipient,
        )

    backend_configuration_values = (app_configuration.configuration_values or {}).get("backend")

    if backend_configuration_values:
        backend_configuration_values = copy.deepcopy(backend_configuration_values)
        backend = service_backends.get_backend(backend_configuration_values.pop("name"))(**backend_configuration_values)
    else:
        # If no backend details are provided, use Google Pub/Sub with the default project.
        _, project_name = auth.default()
        backend = service_backends.get_backend()(project_name=project_name)

    answer = answer_question(
        question=question,
        project_name=backend.project_name,
        service_configuration=service_configuration,
        app_configuration=app_configuration,
    )

    click.echo(json.dumps(answer, cls=OctueJSONEncoder))


@question.group()
def events():
    """Get and replay events from past and current questions."""


@events.command()
@click.option(
    "--question-uuid",
    type=str,
    default=None,
    help="The UUID of the question to get events for.",
)
@click.option(
    "--parent-question-uuid",
    type=str,
    default=None,
    help="The UUID of a parent question to get the sub-question events for.",
)
@click.option(
    "--originator-question-uuid",
    type=str,
    default=None,
    help="The UUID of an originator question get the full tree of events for.",
)
@click.option(
    "-k",
    "--kinds",
    type=str,
    default=None,
    help="The kinds of event to get as a comma-separated list e.g. 'question,result'. If not provided, all event kinds "
    f"are returned. The valid kinds are {VALID_EVENT_KINDS!r}.",
)
@click.option(
    "-e",
    "--exclude-kinds",
    type=str,
    default=None,
    help="The kinds of event to exclude as a comma-separated list e.g. 'question,result'. If not provided, all event "
    f"kinds are returned. The valid kinds are {VALID_EVENT_KINDS!r}.",
)
@click.option(
    "--include-backend-metadata",
    is_flag=True,
    help="Include the service backend metadata.",
)
@click.option(
    "-l",
    "--limit",
    type=int,
    default=1000,
    show_default=True,
    help="Limit the number of events returned.",
)
@click.option(
    "-c",
    "--service-config",
    type=click.Path(dir_okay=False),
    default=None,
    help="The path to an `octue.yaml` file defining the service to run. If not provided, the "
    "`OCTUE_SERVICE_CONFIGURATION_PATH` environment variable is used if present, otherwise the local path `octue.yaml` "
    "is used.",
)
def get(
    question_uuid,
    parent_question_uuid,
    originator_question_uuid,
    kinds,
    exclude_kinds,
    include_backend_metadata,
    limit,
    service_config,
):
    """Get the events emitted during a question as JSON. One of the following must be set:

    --question-uuid\n
    --parent-question-uuid\n
    --originator-question-uuid\n
    """
    if kinds:
        kinds = kinds.split(",")

    if exclude_kinds:
        exclude_kinds = exclude_kinds.split(",")

    service_configuration = ServiceConfiguration.from_file(path=service_config, allow_not_found=True)

    if service_configuration:
        event_store_table_id = service_configuration.event_store_table_id
    else:
        event_store_table_id = DEFAULT_EVENT_STORE_TABLE_ID

    events = get_events(
        table_id=event_store_table_id,
        question_uuid=question_uuid,
        parent_question_uuid=parent_question_uuid,
        originator_question_uuid=originator_question_uuid,
        kinds=kinds,
        exclude_kinds=exclude_kinds,
        include_backend_metadata=include_backend_metadata,
        limit=limit,
    )

    click.echo(json.dumps(events, cls=OctueJSONEncoder))


@events.command()
@click.option(
    "--question-uuid",
    type=str,
    default=None,
    help="The UUID of the question to get events for.",
)
@click.option(
    "--parent-question-uuid",
    type=str,
    help="The UUID of a parent question to get the sub-question events for.",
)
@click.option(
    "--originator-question-uuid",
    type=str,
    help="The UUID of an originator question get the full tree of events for.",
)
@click.option(
    "-k",
    "--kinds",
    type=str,
    default=None,
    help="The kinds of event to get as a comma-separated list e.g. 'question,result'. If not provided, all event kinds "
    f"are returned. The valid kinds are {VALID_EVENT_KINDS!r}.",
)
@click.option(
    "-e",
    "--exclude-kinds",
    type=str,
    default=None,
    help="The kinds of event to exclude as a comma-separated list e.g. 'question,result'. If not provided, all event "
    f"kinds are returned. The valid kinds are {VALID_EVENT_KINDS!r}.",
)
@click.option(
    "-l",
    "--limit",
    type=int,
    default=1000,
    show_default=True,
    help="Limit the number of events returned.",
)
@click.option(
    "-c",
    "--service-config",
    type=click.Path(dir_okay=False),
    default=None,
    help="The path to an `octue.yaml` file defining the service to run. If not provided, the "
    "`OCTUE_SERVICE_CONFIGURATION_PATH` environment variable is used if present, otherwise the local path `octue.yaml` "
    "is used.",
)
@click.option(
    "--include-service-metadata",
    is_flag=True,
    help="Include the SRUIDs and question UUIDs of the service revisions involved in the question at the start of each "
    "log message. This is useful when a child asks its own sub-questions.",
)
@click.option(
    "--exclude-logs-containing",
    type=str,
    default=None,
    help="Skip handling log messages containing this string.",
)
@click.option(
    "--validate-events",
    is_flag=True,
    help="Validate events before attempting to handle them (this is off by default to speed up event handling)",
)
def replay(
    question_uuid,
    parent_question_uuid,
    originator_question_uuid,
    kinds,
    exclude_kinds,
    limit,
    service_config,
    include_service_metadata,
    exclude_logs_containing,
    validate_events,
):
    """Replay a question's events, returning the result as JSON at the end if there is one. One of the following must be
    set:

    --question-uuid\n
    --parent-question-uuid\n
    --originator-question-uuid\n
    """
    if kinds:
        kinds = kinds.split(",")

    if exclude_kinds:
        exclude_kinds = exclude_kinds.split(",")

    service_configuration = ServiceConfiguration.from_file(path=service_config, allow_not_found=True)

    if service_configuration:
        event_store_table_id = service_configuration.event_store_table_id
    else:
        event_store_table_id = DEFAULT_EVENT_STORE_TABLE_ID

    events = get_events(
        table_id=event_store_table_id,
        question_uuid=question_uuid,
        parent_question_uuid=parent_question_uuid,
        originator_question_uuid=originator_question_uuid,
        kinds=kinds,
        exclude_kinds=exclude_kinds,
        limit=limit,
    )

    if not events:
        return

    replayer = EventReplayer(
        include_service_metadata_in_logs=include_service_metadata,
        exclude_logs_containing=exclude_logs_containing,
        validate_events=validate_events,
    )

    result = replayer.handle_events(events)

    if not result:
        return

    click.echo(json.dumps(result, cls=OctueJSONEncoder))


@question.command()
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
    help="If provided, download any datasets from the diagnostics and update their paths in the configuration and "
    "input manifests to the new local paths.",
)
def diagnostics(cloud_path, local_path, download_datasets):
    """Download diagnostics for a question from the given directory in Google Cloud Storage. The cloud path should end
    in the question ID.

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

    local_paths = GoogleCloudStorageClient().download_all_files(
        local_path=local_path,
        cloud_path=cloud_path,
        filter=filter,
        recursive=True,
    )

    if not local_paths:
        logger.warning("No diagnostics found at %r.", cloud_path)
        return

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

    logger.info("Downloaded diagnostics from %r to %r.", cloud_path, local_path)


# @question.command()
# @click.argument("question_uuid", type=str)
# @click.option(
#     "-p",
#     "--project-name",
#     type=str,
#     default=None,
#     help="If asking a remote question, the name of the Google Cloud project the service is deployed in. If not "
#     "provided, the project name is detected from the local Google application credentials if present.",
# )
# @click.option(
#     "-c",
#     "--service-config",
#     type=click.Path(dir_okay=False),
#     default=None,
#     help="An optional path to an `octue.yaml` file defining service registries to use. If not provided, the "
#     "`OCTUE_SERVICE_CONFIGURATION_PATH` environment variable is used if present, otherwise the local path `octue.yaml` "
#     "is used.",
# )
# def cancel(question_uuid, project_name, service_config):
#     """Cancel a question running on an Octue Twined service.
#
#     QUESTION_UUID: The question UUID of a running question
#     """
#     service_configuration = ServiceConfiguration.from_file(path=service_config)
#
#     if not project_name:
#         _, project_name = auth.default()
#
#     child = Child(id=None, backend={"name": "GCPPubSubBackend", "project_name": project_name})
#     child.cancel(question_uuid=question_uuid, event_store_table_id=service_configuration.event_store_table_id)


@octue_cli.command(deprecated=True)
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
    help="If provided, download any datasets from the diagnostics and update their paths in the configuration and "
    "input manifests to the new local paths.",
)
def get_diagnostics(cloud_path, local_path, download_datasets):
    diagnostics(cloud_path, local_path, download_datasets)


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
    service_namespace, service_name, service_revision_tag = get_sruid_parts(service_configuration)

    if service_revision_tag_override and service_revision_tag:
        logger.warning(
            "The `OCTUE_SERVICE_REVISION_TAG` environment variable %r has been overridden by the `--revision-tag` CLI "
            "option %r.",
            service_revision_tag,
            service_revision_tag_override,
        )

    service_sruid = create_sruid(
        namespace=service_namespace,
        name=service_name,
        revision_tag=service_revision_tag_override or service_revision_tag,
    )

    runner = Runner.from_configuration(
        service_configuration=service_configuration,
        app_configuration=app_configuration,
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
        service_sruid = create_sruid(namespace=service_namespace, name=service_name)

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
