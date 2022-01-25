import logging
import os

import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
from apache_beam.runners.dataflow.internal.apiclient import DataflowJobAlreadyExistsError

from octue import REPOSITORY_ROOT
from octue.cloud.deployment.google.answer_pub_sub_question import answer_question
from octue.cloud.pub_sub import Topic
from octue.cloud.pub_sub.service import Service
from octue.exceptions import DeploymentError
from octue.resources.service_backends import GCPPubSubBackend


logger = logging.getLogger(__name__)


DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION = "gs://octue-sdk-python-dataflow-temporary-data/temp"
DEFAULT_SETUP_FILE_PATH = os.path.join(REPOSITORY_ROOT, "setup.py")


def create_streaming_job(
    service_name,
    service_id,
    project_name,
    region,
    image_uri,
    runner="DataflowRunner",
    setup_file_path=DEFAULT_SETUP_FILE_PATH,
    temporary_files_location=DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION,
    update=False,
    extra_options=None,
):
    """Deploy an `octue` service as a streaming Google Dataflow Prime job.

    :param str service_name: the name to give the Dataflow job
    :param str service_id: the Pub/Sub topic name for the Dataflow job to subscribe to
    :param str project_name: the name of the project to deploy the job to
    :param str region: the region to deploy the job to
    :param str image_uri: the URI of the `apache-beam`-based Docker image to use for the job
    :param str runner: the name of an `apache-beam` runner to use to execute the job
    :param str setup_file_path: path to the python `setup.py` file to use for the job
    :param str temporary_files_location: a Google Cloud Storage path to save temporary files from the job at
    :param bool update: if `True`, update the existing job with the same name
    :param iter|None extra_options: any further arguments in command-line-option format to be passed to Apache Beam as pipeline options
    :raise DeploymentError: if a Dataflow job with the service name already exists
    :return None:
    """
    beam_args = [
        f"--project={project_name}",
        f"--region={region}",
        f"--temp_location={temporary_files_location}",
        f"--job_name={service_name}",
        f"--runner={runner}",
        f"--sdk_container_image={image_uri}",
        f"--setup_file={os.path.abspath(setup_file_path)}",
        "--dataflow_service_options=enable_prime",
        "--streaming",
        *(extra_options or []),
    ]

    if update:
        beam_args.append("--update")

    options = PipelineOptions(beam_args)
    pipeline = apache_beam.Pipeline(options=options)

    service_topic = Topic(name=service_id, service=Service(backend=GCPPubSubBackend(project_name=project_name)))
    service_topic.create(allow_existing=True)

    (
        pipeline
        | "Read from Pub/Sub" >> apache_beam.io.ReadFromPubSub(topic=service_topic.path, with_attributes=True)
        | "Answer question" >> apache_beam.Map(lambda question: answer_question(question, project_name=project_name))
    )

    try:
        DataflowRunner().run_pipeline(pipeline, options=options)
    except DataflowJobAlreadyExistsError:
        raise DeploymentError(f"A Dataflow job with name {service_name!r} already exists.") from None
