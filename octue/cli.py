import importlib
import os
import pkg_resources
import sys
from functools import update_wrapper
import click

from octue import exceptions


FOLDER_DEFAULTS = {
    "configuration": "configuration",
    "input": "input",
    "tmp": "tmp",
    "output": "output",
}

VALUES_FILENAME = "values.json"
MANIFEST_FILENAME = "manifest.json"


def get_version():
    return pkg_resources.get_distribution("octue").version


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
@click.version_option(version=get_version())
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


def octue_run(f):
    """ Decorator for the main `run` function which adds a command to the CLI and prepares analysis ready for the run
    """

    @octue_cli.command()
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
    def run(analysis, data_dir, config_dir, input_dir, tmp_dir, output_dir):
        if not config_dir:
            config_dir = os.path.join(data_dir, FOLDER_DEFAULTS["configuration"])
        if not input_dir:
            input_dir = os.path.join(data_dir, FOLDER_DEFAULTS["input"])
        if not tmp_dir:
            tmp_dir = os.path.join(data_dir, FOLDER_DEFAULTS["tmp"])
        if not output_dir:
            output_dir = os.path.join(data_dir, FOLDER_DEFAULTS["output"])

        for filename in VALUES_FILENAME, MANIFEST_FILENAME:
            if not file_in_directory(filename, config_dir):
                raise exceptions.FileNotFoundException(f"No file named {filename} file found in {config_dir}")

            if not file_in_directory(filename, input_dir):
                raise exceptions.FileNotFoundException(f"No file named {filename} file found in {config_dir}")

        analysis.config_values = os.path.join(config_dir, VALUES_FILENAME)
        analysis.input_values = os.path.join(input_dir, VALUES_FILENAME)
        analysis.output_values = os.path.join(output_dir, VALUES_FILENAME)
        return f(analysis)

    return update_wrapper(run, f)

def file_in_directory(filename, directory):
    return os.path.isfile(os.path.join(directory, filename))


def unwrap(fcn):
    """ Recurse through wrapping to get the raw function without decorators.
    """
    if hasattr(fcn, "__wrapped__"):
        return unwrap(fcn.__wrapped__)
    return fcn


class AppFrom:
    """ Context manager that allows us to temporarily add an app's location to the system path and
    extract its run function

    with AppFrom('/path/to/dir') as app:
        Runner().run(app)

    """

    def __init__(self, app_path="."):
        self.app_path = os.path.abspath(os.path.normpath(app_path))
        self.app_module = None

    def __enter__(self):
        sys.path.insert(0, self.app_path)
        self.app_module = importlib.import_module("app")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.app_path in sys.path:
            sys.path.remove(self.app_path)

    @property
    def run(self):
        """ Returns the unwrapped run function from app.py in the application's root directory
        """
        return unwrap(self.app_module.run)


if __name__ == "__main__":
    args = sys.argv[1:] if len(sys.argv) > 1 else []
    octue_cli(args)
