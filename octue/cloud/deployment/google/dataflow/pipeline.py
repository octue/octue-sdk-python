import logging
import os

import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
from apache_beam.runners.dataflow.internal.apiclient import DataflowJobAlreadyExistsError

from octue import REPOSITORY_ROOT
from octue.cloud.deployment.google.answer_pub_sub_question import answer_question
from octue.cloud.pub_sub import Topic
from octue.cloud.pub_sub.service import OCTUE_NAMESPACE, Service
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
    setup_file_path=DEFAULT_SETUP_FILE_PATH,
    temporary_files_location=DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION,
    service_account_email=None,
    worker_machine_type=None,
    maximum_instances=None,
    update=False,
    extra_options=None,
):
    """Deploy an `octue` service as a streaming Google Dataflow Prime job.

    :param str service_name: the name to give the Dataflow job
    :param str service_id: the Pub/Sub topic name for the Dataflow job to subscribe to
    :param str project_name: the name of the project to deploy the job to
    :param str region: the region to deploy the job to
    :param str image_uri: the URI of the `apache-beam`-based Docker image to use for the job
    :param str setup_file_path: path to the python `setup.py` file to use for the job
    :param str temporary_files_location: a Google Cloud Storage path to save temporary files from the job at
    :param str|None service_account_email: the email of the service account to run the Dataflow VMs as
    :param str|None worker_machine_type: the machine type to create Dataflow worker VMs as. See https://cloud.google.com/compute/docs/machine-types for a list of valid options. If not set, the Dataflow service will choose a reasonable default.
    :param int|None maximum_instances: the maximum number of workers to use when executing the Dataflow job
    :param bool update: if `True`, update the existing job with the same name
    :param dict|None extra_options: any further arguments to be passed to Apache Beam as pipeline options
    :raise DeploymentError: if a Dataflow job with the service name already exists
    :return None:
    """
    pipeline_options = {
        "project": project_name,
        "region": region,
        "temp_location": temporary_files_location,
        "job_name": service_name,
        "sdk_container_image": image_uri,
        "setup_file": os.path.abspath(setup_file_path),
        "update": update,
        "streaming": True,
        **(extra_options or {}),
    }

    if service_account_email:
        pipeline_options["service_account_email"] = service_account_email

    if worker_machine_type:
        pipeline_options["worker_machine_type"] = worker_machine_type
    else:
        # Dataflow Prime can only be used if a worker machine type is not specified.
        pipeline_options["dataflow_service_options"] = ["enable_prime"]

    if maximum_instances:
        pipeline_options["max_num_workers"] = maximum_instances

    pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
    pipeline = apache_beam.Pipeline(options=pipeline_options)

    service_topic = Topic(
        name=service_id,
        namespace=OCTUE_NAMESPACE,
        service=Service(backend=GCPPubSubBackend(project_name=project_name)),
    )

    service_topic.create(allow_existing=True)

    (
        pipeline
        | "Read from Pub/Sub" >> apache_beam.io.ReadFromPubSub(topic=service_topic.path, with_attributes=True)
        | "Answer question" >> apache_beam.Map(lambda question: answer_question(question, project_name=project_name))
    )

    try:
        DataflowRunner().run_pipeline(pipeline, options=pipeline_options)
    except DataflowJobAlreadyExistsError:
        raise DeploymentError(f"A Dataflow job with name {service_name!r} already exists.") from None
