import logging
import os
import uuid
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from octue import REPOSITORY_ROOT
from octue.cloud.deployment.google.answer_pub_sub_question import answer_question
from octue.cloud.pub_sub import Topic


logger = logging.getLogger(__name__)


DEFAULT_IMAGE_URI = "eu.gcr.io/octue-amy/octue-sdk-python:latest"
DATAFLOW_TEMPORARY_FILES_LOCATION = "gs://pub-sub-dataflow-trial/temp"


def deploy_streaming_pipeline(
    service_id,
    project_name,
    region,
    runner="DataflowRunner",
    image_uri=DEFAULT_IMAGE_URI,
    extra_options=None,
):
    """Deploy a streaming Dataflow pipeline to Google Cloud.

    :param str service_id:
    :param str project_name:
    :param str temporary_files_cloud_path:
    :param str region:
    :param str runner:
    :param str|None image_uri:
    :param iter|None extra_options: any further arguments in command-line-option format to be passed to Apache Beam as pipeline options
    :return None:
    """
    beam_args = [
        f"--project={project_name}",
        f"--runner={runner}",
        f"--temp_location={DATAFLOW_TEMPORARY_FILES_LOCATION}",
        f"--region={region}",
        "--dataflow_service_options=enable_prime",
        f"--setup_file={os.path.join(REPOSITORY_ROOT, 'setup.py')}",
        *(extra_options or []),
    ]

    if image_uri:
        beam_args.append(f"--sdk_container_image={image_uri}")

    service_id = Topic.generate_topic_path(project_name, service_id)

    with beam.Pipeline(options=PipelineOptions(beam_args, streaming=True)) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=service_id, with_attributes=True)
            | "Answer question" >> beam.Map(lambda question: answer_question(question, project_name=project_name))
        )


def dispatch_batch_job(
    data,
    project_name,
    region,
    runner="DataflowRunner",
    image_uri=DEFAULT_IMAGE_URI,
    forward_logs=True,
    extra_options=None,
):
    question = {"data": data, "attributes": {"question_uuid": str(uuid.uuid4()), "forward_logs": forward_logs}}

    beam_args = [
        f"--project={project_name}",
        f"--runner={runner}",
        f"--temp_location={DATAFLOW_TEMPORARY_FILES_LOCATION}",
        f"--region={region}",
        f"--setup_file={os.path.join(REPOSITORY_ROOT, 'setup.py')}",
        "--experiments=use_runner_v2",
        "--dataflow_service_options=enable_prime",
        *(extra_options or []),
    ]

    if image_uri:
        beam_args.append(f"--sdk_container_image={image_uri}")

    with beam.Pipeline(options=PipelineOptions(beam_args, streaming=False)) as pipeline:
        (
            pipeline
            | "Send data to Dataflow" >> beam.Create([question])
            | "Answer question" >> beam.Map(lambda question: answer_question(question, project_name=project_name))
        )
