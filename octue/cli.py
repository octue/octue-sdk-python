import os
import pkg_resources
import sys
import click

from octue import exceptions
from octue.runner import Runner


FOLDER_DEFAULTS = {
    "configuration": "configuration",
    "input": "input",
    "tmp": "tmp",
    "output": "output",
}

VALUES_FILENAME = "values.json"
MANIFEST_FILENAME = "manifest.json"


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--id",
    default=None,
    show_default=True,
    help="Id of the analysis being undertaken. None (for local use) will cause a unique ID to be generated.",
)
@click.option(
    "--skip-checks/--no-skip-checks",
    default=False,
    is_flag=True,
    show_default=True,
    help="Skips the input checking. This can be a timesaver if you already checked "
    "data directories (especially if manifests are large).",
)
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
@click.pass_context
def octue_cli(ctx, id, skip_checks, log_level, force_reset):
    """ Octue CLI, enabling a data service / digital twin to be run like a command line application.

    Provide sources of configuration and/or input data and run the app. A source can be:

    - A path (relative or absolute) to a directory containing a <strand>.json file (eg `path/to/dir`).
    - A path to a <strand>.json file (eg `path/to/configuration_values.json`).
    - A literal JSON string (eg `{"n_iterations": 10}`.

    """
    ctx.ensure_object(dict)
    ctx.obj["analysis"] = "VIBRATION"


@octue_cli.command()
@click.option(
    "--app-dir",
    type=click.Path(),
    default=".",
    show_default=True,
    help="Directory containing source code for custom Octue app."
)
@click.option(
    "--data-dir",
    type=click.Path(),
    default=".",
    show_default=True,
    help="Location of directories containing configuration values and manifest, input values and manifest, and output "
         "directory."
)
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    show_default=True,
    help="Directory containing configuration.",
)
@click.option(
    "--input-dir",
    type=click.Path(),
    default=None,
    show_default=True,
    help="Directory containing input.",
)
@click.option(
    "--tmp-dir",
    type=click.Path(),
    default=None,
    show_default=True,
    help="Directory to store intermediate files in.",
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    show_default=True,
    help="Directory to write outputs as files.",
)
@click.option(
    "--twine",
    type=click.Path(),
    default="twine.json",
    show_default=True,
    help="Location of Twine file.",
)
def run(app_dir, data_dir, config_dir, input_dir, tmp_dir, output_dir, twine):
    config_dir = config_dir or os.path.join(data_dir, FOLDER_DEFAULTS["configuration"])
    input_dir = input_dir or os.path.join(data_dir, FOLDER_DEFAULTS["input"])
    tmp_dir = tmp_dir or os.path.join(data_dir, FOLDER_DEFAULTS["tmp"])
    output_dir = output_dir or os.path.join(data_dir, FOLDER_DEFAULTS["output"])

    for filename in VALUES_FILENAME, MANIFEST_FILENAME:
        if not file_in_directory(filename, config_dir):
            raise exceptions.FileNotFoundException(f"No file named {filename} file found in {config_dir}")

        if not file_in_directory(filename, input_dir):
            raise exceptions.FileNotFoundException(f"No file named {filename} file found in {input_dir}")

    runner = Runner(twine=twine, configuration_values=os.path.join(config_dir, VALUES_FILENAME))
    runner.run(app_src=app_dir, input_values=os.path.join(input_dir, VALUES_FILENAME))


def file_in_directory(filename, directory):
    return os.path.isfile(os.path.join(directory, filename))


if __name__ == "__main__":
    args = sys.argv[1:] if len(sys.argv) > 1 else []
    octue_cli(args)
