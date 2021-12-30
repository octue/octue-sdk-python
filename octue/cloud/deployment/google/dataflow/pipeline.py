import json
import logging
import os
import apache_beam as beam  # Google Dataflow provides `apache_beam` as a dependency - it is not needed as a dependency for octue-sdk-python or in the Dockerfile used with Dataflow.
from apache_beam.options.pipeline_options import PipelineOptions  # noqa

from octue import REPOSITORY_ROOT
from octue.cloud.deployment.google.dataflow.answer_question import answer_question
from octue.cloud.pub_sub import Topic


logger = logging.getLogger(__name__)


def run(input_topic, output_topic, beam_args=None):
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=input_topic, with_attributes=True)
            | "Transform question to dictionary"
            >> beam.Map(
                lambda question: {**json.loads(question.data.decode("utf-8")), "attributes": question.attributes}
            )
            | "Answer question" >> beam.Map(lambda question: answer_question(question, project_name="octue-amy"))
            | "Encode as bytes" >> beam.Map(lambda answer: json.dumps(answer).encode("utf-8"))
            | "Write to Pub/Sub" >> beam.io.WriteToPubSub(topic=output_topic)
        )


def deploy_pipeline(input_topic, output_topic, project_name, temporary_files_cloud_path, region, runner, image_uri):
    run(
        input_topic=Topic.generate_topic_path(project_name, input_topic),
        output_topic=Topic.generate_topic_path(project_name, output_topic),
        beam_args=[
            f"--project={project_name}",
            f"--runner={runner}",
            f"--temp_location={temporary_files_cloud_path}",
            f"--region={region}",
            f"--sdk_container_image={image_uri}",
            f"--setup_file={os.path.join(REPOSITORY_ROOT, 'setup.py')}",
        ],
    )


if __name__ == "__main__":
    deploy_pipeline(
        project_name="octue-amy",
        input_topic="octue.services.pub-sub-dataflow-trial",
        output_topic="octue.services.pub-sub-dataflow-trial.answers.92505713-6052-4e17-b34e-848225ab3acc",
        temporary_files_cloud_path="gs://pub-sub-dataflow-trial/temp",
        region="europe-west2",
        runner="DataflowRunner",
        image_uri="eu.gcr.io/octue-amy/octue-sdk-python:latest",
    )
