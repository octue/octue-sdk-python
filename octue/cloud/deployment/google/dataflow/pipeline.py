import json
import logging
import apache_beam as beam  # Google Dataflow provides `apache_beam` as a dependency - it is not needed as a dependency for octue-sdk-python or in the Dockerfile used with Dataflow.
from apache_beam.options.pipeline_options import PipelineOptions  # noqa


logger = logging.getLogger(__name__)


def run(input_topic, output_topic, beam_args=None):
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=input_topic, with_attributes=True)
            | "Add dummy output data"
            >> beam.Map(
                lambda question: {
                    "type": "result",
                    "output_values": f"The Dataflow analysis works! Original question: {question.data!r}; attributes: {question.attributes!r}",
                    "output_manifest": None,
                    "message_number": 0,
                }
            )
            | "Encode as bytes" >> beam.Map(lambda answer: json.dumps(answer).encode("utf-8"))
            | "Write to Pub/Sub" >> beam.io.WriteToPubSub(topic=output_topic)
        )


def deploy_pipeline(input_topic, output_topic, project_name, temporary_files_cloud_path, region):
    run(
        input_topic=f"projects/{project_name}/topics/{input_topic}",
        output_topic=f"projects/{project_name}/topics/{output_topic}",
        beam_args=[
            f"--project={project_name}",
            "--runner=DataflowRunner",
            f"--temp_location={temporary_files_cloud_path}",
            f"--region={region}",
        ],
    )


if __name__ == "__main__":
    deploy_pipeline(
        project_name="octue-amy",
        input_topic="pub-sub-dataflow-trial",
        output_topic="dataflow-output-topic",
        temporary_files_cloud_path="gs://pub-sub-dataflow-trial/temp",
        region="europe-west2",
    )
