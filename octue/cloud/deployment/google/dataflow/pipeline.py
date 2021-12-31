import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from octue import REPOSITORY_ROOT
from octue.cloud.deployment.google.dataflow.answer_question import answer_question
from octue.cloud.pub_sub import Topic


logger = logging.getLogger(__name__)


DEFAULT_IMAGE_URI = "eu.gcr.io/octue-amy/octue-sdk-python:latest"


def deploy_streaming_pipeline(
    input_topic_name,
    project_name,
    temporary_files_cloud_path,
    region,
    runner="DataflowRunner",
    image_uri=DEFAULT_IMAGE_URI,
    extra_options=None,
):
    """Deploy a streaming Dataflow pipeline to Google Cloud.

    :param str input_topic_name:
    :param str project_name:
    :param str temporary_files_cloud_path:
    :param str region:
    :param str runner:
    :param str image_uri:
    :param iter|None extra_options: any further arguments in command-line-option format to be passed to Apache Beam as pipeline options
    :return None:
    """
    beam_args = [
        f"--project={project_name}",
        f"--runner={runner}",
        f"--temp_location={temporary_files_cloud_path}",
        f"--region={region}",
        f"--sdk_container_image={image_uri}",
        f"--setup_file={os.path.join(REPOSITORY_ROOT, 'setup.py')}",
        *extra_options,
    ]

    input_topic = Topic.generate_topic_path(project_name, input_topic_name)

    with beam.Pipeline(options=PipelineOptions(beam_args, streaming=True)) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=input_topic, with_attributes=True)
            | "Answer question" >> beam.Map(lambda question: answer_question(question, project_name=project_name))
        )
