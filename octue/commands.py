import functools
import os

from octue.definitions import CHILDREN_FILENAME, FOLDER_DEFAULTS, MANIFEST_FILENAME, VALUES_FILENAME
from octue.resources.communication import Service, service_backends
from octue.runner import Runner
from twined import Twine


def run(
    app_dir,
    data_dir,
    config_dir,
    input_dir,
    output_dir,
    twine,
    analysis_id,
    log_level,
    log_handler,
    show_twined_logs,
    skip_checks,
):
    config_dir = config_dir or os.path.join(data_dir, FOLDER_DEFAULTS["configuration"])
    input_dir = input_dir or os.path.join(data_dir, FOLDER_DEFAULTS["input"])
    output_dir = output_dir or os.path.join(data_dir, FOLDER_DEFAULTS["output"])

    twine = Twine(source=twine)

    (
        configruation_values,
        configuration_manifest,
        input_values,
        input_manifest,
        children,
    ) = set_unavailable_strand_paths_to_none(
        twine,
        (
            ("configuration_values", os.path.join(config_dir, VALUES_FILENAME)),
            ("configuration_manifest", os.path.join(config_dir, MANIFEST_FILENAME)),
            ("input_values", os.path.join(input_dir, VALUES_FILENAME)),
            ("input_manifest", os.path.join(input_dir, MANIFEST_FILENAME)),
            ("children", os.path.join(config_dir, CHILDREN_FILENAME)),
        ),
    )

    runner = Runner(
        twine=twine,
        configuration_values=configruation_values,
        configuration_manifest=configuration_manifest,
        log_level=log_level,
        handler=log_handler,
        show_twined_logs=show_twined_logs,
    )

    analysis = runner.run(
        app_src=app_dir,
        analysis_id=analysis_id,
        input_values=input_values,
        input_manifest=input_manifest,
        children=children,
        output_manifest_path=os.path.join(output_dir, MANIFEST_FILENAME),
        skip_checks=skip_checks,
    )
    analysis.finalise(output_dir=output_dir)


def start(
    app_dir,
    data_dir,
    config_dir,
    service_id,
    twine,
    timeout,
    log_level,
    log_handler,
    show_twined_logs,
    skip_checks,
    delete_topic_and_subscription_on_exit,
):
    config_dir = config_dir or os.path.join(data_dir, FOLDER_DEFAULTS["configuration"])
    twine = Twine(source=twine)

    configuration_values, configuration_manifest, children = set_unavailable_strand_paths_to_none(
        twine,
        (
            ("configuration_values", os.path.join(config_dir, VALUES_FILENAME)),
            ("configuration_manifest", os.path.join(config_dir, MANIFEST_FILENAME)),
            ("children", os.path.join(config_dir, CHILDREN_FILENAME)),
        ),
    )

    runner = Runner(
        twine=twine,
        configuration_values=configuration_values,
        configuration_manifest=configuration_manifest,
        log_level=log_level,
        handler=log_handler,
        show_twined_logs=show_twined_logs,
    )

    run_function = functools.partial(runner.run, app_src=app_dir, children=children, skip_checks=skip_checks)

    backend_configuration_values = runner.configuration["configuration_values"]["backend"]
    backend = service_backends.get_backend(backend_configuration_values.pop("name"))(**backend_configuration_values)

    service = Service(id=service_id, backend=backend, run_function=run_function)
    service.serve(timeout=timeout, delete_topic_and_subscription_on_exit=delete_topic_and_subscription_on_exit)


def set_unavailable_strand_paths_to_none(twine, strands):
    """ Set paths to unavailable strands to None, leaving the paths of available strands as they are. """
    updated_strand_paths = []

    for strand_name, strand in strands:
        if strand_name not in twine.available_strands:
            updated_strand_paths.append(None)
        else:
            updated_strand_paths.append(strand)

    return updated_strand_paths
