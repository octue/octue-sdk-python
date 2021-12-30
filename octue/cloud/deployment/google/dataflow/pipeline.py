import json
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from octue import REPOSITORY_ROOT
from octue.cloud.deployment.google.dataflow.answer_question import answer_question
from octue.cloud.pub_sub import Topic


logger = logging.getLogger(__name__)


DEFAULT_IMAGE_URI = "eu.gcr.io/octue-amy/octue-sdk-python:latest"


def deploy_pipeline(
    input_topic_name,
    output_topic_name,
    project_name,
    temporary_files_cloud_path,
    region,
    runner="DataflowRunner",
    image_uri=DEFAULT_IMAGE_URI,
    *args,
):
    """Deploy a streaming Dataflow pipeline to Google Cloud.

    :param str input_topic_name:
    :param str output_topic_name:
    :param str project_name:
    :param str temporary_files_cloud_path:
    :param str region:
    :param str runner:
    :param str image_uri:
    :param args: any further arguments in command-line-option format to be passed to Apache Beam as pipeline options
    :return None:
    """
    beam_args = [
        f"--project={project_name}",
        f"--runner={runner}",
        f"--temp_location={temporary_files_cloud_path}",
        f"--region={region}",
        f"--sdk_container_image={image_uri}",
        f"--setup_file={os.path.join(REPOSITORY_ROOT, 'setup.py')}",
        *args,
    ]

    input_topic = Topic.generate_topic_path(project_name, input_topic_name)
    output_topic = Topic.generate_topic_path(project_name, output_topic_name)

    with beam.Pipeline(options=PipelineOptions(beam_args, save_main_session=True, streaming=True)) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=input_topic, with_attributes=True)
            | "Transform question to dictionary"
            >> beam.Map(
                lambda question: {**json.loads(question.data.decode("utf-8")), "attributes": question.attributes}
            )
            | "Answer question" >> beam.Map(lambda question: answer_question(question, project_name=project_name))
            | "Encode as bytes" >> beam.Map(lambda answer: json.dumps(answer).encode("utf-8"))
            | "Write to Pub/Sub" >> beam.io.WriteToPubSub(topic=output_topic)
        )


if __name__ == "__main__":
    deploy_pipeline(
        project_name="octue-amy",
        input_topic_name="octue.services.pub-sub-dataflow-trial",
        output_topic_name="octue.services.pub-sub-dataflow-trial.answers.92505713-6052-4e17-b34e-848225ab3acc",
        temporary_files_cloud_path="gs://pub-sub-dataflow-trial/temp",
        region="europe-west2",
    )
