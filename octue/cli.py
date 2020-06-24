import click
from octue.resources import analysis


@click.group()
@click.option(
    "--id",
    default=None,
    show_default=True,
    help="Id of the analysis being undertaken. None (for local use) prevents "
    "registration of the analysis or any of the results.",
)
# TODO Add --log-level option
@click.option(
    "--skip-checks/--no-skip-checks",
    default=False,
    is_flag=True,
    show_default=True,
    help="Skips the input checking. This can be a timesaver if you already checked "
    "the data directory once, and its a big manifest.",
)
@click.option(
    "--force-reset/--no-force-reset",
    default=True,
    is_flag=True,
    show_default=True,
    help="Forces a reset of analysis cache and outputs [For future use, currently not implemented)",
)
@click.option(
    "--data-dir",
    type=click.Path(),
    default=".",
    show_default=True,
    help="Absolute or relative path to the data directory (which will contain config, manifest files "
    "and <data_dir>/input <data_dir>/output folders).",
)
@click.option(
    "--input-dir",
    type=click.Path(),
    default="<data-dir>/input",
    show_default=True,
    help="Absolute or relative path to a folder containing input files.",
)
@click.option(
    "--tmp-dir",
    type=click.Path(),
    default="<data-dir>/tmp",
    show_default=True,
    help="Absolute or relative path to a folder for temporary files, where you can save "
    "cache files during your computation. This cache lasts the duration of the analysis "
    "and may not be available beyond the end of an analysis.",
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default="<data-dir>/output",
    show_default=True,
    help="Absolute or relative path to a folder where results should be saved.",
)
@click.option(
    "--log-dir",
    type=click.Path(),
    default="<data-dir>/logs",
    show_default=True,
    help="Path to the location of log files",
)
def octue_app(id, skip_checks, force_reset, data_dir, input_dir, tmp_dir, output_dir, log_dir):
    """Creates the CLI for an Octue application
    """

    # We want to show meaningful defaults in the CLI help but unfortunately have to strip out the real values here
    if input_dir.startswith("<data-dir>/"):
        input_dir = None
    if log_dir.startswith("<data-dir>/"):
        log_dir = None
    if output_dir.startswith("<data-dir>/"):
        output_dir = None
    if tmp_dir.startswith("<data-dir>/"):
        tmp_dir = None

    # Use the setup method to update the existing analysis
    analysis.setup(
        id=id,
        skip_checks=skip_checks,
        data_dir=data_dir,
        input_dir=input_dir,
        tmp_dir=tmp_dir,
        output_dir=output_dir,
        log_dir=log_dir,
    )
