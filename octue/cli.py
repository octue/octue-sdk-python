import importlib
import os
import sys
from functools import update_wrapper
import click


FOLDERS = (
    "configuration",
    "input",
    "log",
    "tmp",
    "output",
)


@click.group()
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
@click.option(
    "--configuration-values",
    type=click.Path(),
    default="<data-dir>/input/input_values.json",
    show_default=True,
    help="Source for configuration_values strand data.",
)
@click.option(
    "--configuration-manifest",
    type=click.Path(),
    default="<data-dir>/input/input_values.json",
    show_default=True,
    help="Source for configuration_manifest strand data.",
)
@click.option(
    "--input-values",
    type=click.Path(),
    default="<data-dir>/input/input_values.json",
    show_default=True,
    help="Source for input_values strand data.",
)
@click.option(
    "--input-manifest",
    type=click.Path(),
    default="<data-dir>/input/input_manifest.json",
    show_default=True,
    help="Source for input_manifest strand data.",
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default="output",
    show_default=False,
    help="Directory to write outputs as files.",
)
@click.option(
    "--log-dir", type=click.Path(), default="logs", show_default=True, help="Path to the location of log files",
)
@click.pass_context
def octue_cli(
    ctx,
    id,
    skip_checks,
    log_level,
    force_reset,
    configuration_values,
    configuration_manifest,
    input_values,
    input_manifest,
    data_dir,
    input_dir,
    tmp_dir,
    output_dir,
    log_dir,
):
    """ Octue CLI, enabling a data service / digital twin to be run like a command line application.

    Provide sources of configuration and/or input data and run the app. A source can be:

    - A path (relative or absolute) to a directory containing a <strand>.json file (eg `path/to/dir`).
    - A path to a <strand>.json file (eg `path/to/configuration_values.json`).
    - A literal JSON string (eg `{"n_iterations": 10}`.

    """

    # We want to show meaningful defaults in the CLI help but unfortunately have to strip out the displayed values here
    if input_values.startswith("<data-dir>/"):
        input_dir = None  # noqa
    if log_dir.startswith("<data-dir>/"):
        log_dir = None  # noqa
    if output_dir.startswith("<data-dir>/"):
        output_dir = None  # noqa

    ctx.ensure_object(dict)
    ctx.obj["analysis"] = "VIBRATION"


def pass_analysis(f):
    @click.pass_context
    def new_func(ctx, *args, **kwargs):
        return ctx.invoke(f, ctx.obj["analysis"], *args, **kwargs)

    return update_wrapper(new_func, f)


def octue_run(f):
    """ Decorator for the main `run` function which adds a command to the CLI and prepares analysis ready for the run
    """

    @octue_cli.command()
    @pass_analysis
    def run(*args, **kwargs):
        return f(*args, **kwargs)

    return update_wrapper(run, f)


def octue_version(f):
    """ Decorator for the main `version` function which adds a command to the CLI
    """

    @octue_cli.command()
    def version(*args, **kwargs):
        return f(*args, **kwargs)

    return update_wrapper(version, f)


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
