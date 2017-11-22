from .exceptions import InvalidOctueFileType
import click


class DataFile(object):

    id = None
    name = None
    tags = list()
    description = None


class Analysis(object):

    input_files = list()
    input_tags = list()
    id = None

    def __init__(self):
        pass

    def parse_input_files(self, mapfile):
        # TODO parse a json structure
        pass

    def init_log(self, logfile):
        # TODO use the logger as in amy to mirror output between terminal and logfile
        pass


octue_analysis = click.make_pass_decorator(Analysis, ensure=True)


@click.group()
@click.option('--id', default=None, help='Id of the analysis being undertaken. None (for local use) prevents registration of the analysis or any of the results.')
@click.option('--verbose', is_flag=True, help='Adds the verbose flag to the analysis configuration.')
@click.option('--input-dir', type=click.Path(), default='./input', help='Absolute or relative path to a folder containing input files.')
@click.option('--tmp-dir', type=click.Path(), default='./tmp', help='Absolute or relative path to a folder for temporary files, where you can save cache files during your computation. This cache lasts the duration of the analysis and may not be available beyond the end of an analysis.')
@click.option('--output-dir', type=click.Path(), default='./output', help='Absolute or relative path to a folder where results should be saved.')
@click.option('--log-dir', type=click.Path(), default='./logs', help='Path to the location of log files')
@click.argument('mapfile', type=click.File('r'), required=True)

@octue_analysis
def octue_app(analysis, id, verbose, input_dir, tmp_dir, output_dir, log_dir, mapfile):
    """Creates the CLI for an Octue application
    """

    # Set analysis properties
    analysis.id = id
    analysis.verbose = verbose
    analysis.tmp_dir = tmp_dir
    analysis.output_dir = output_dir

    # Read the mapfile and parse it into a manifest
    analysis.parse_input_files(mapfile)

    # Create a logger
    analysis.init_log(log_dir)


def add_to_manifest(file_type, file_name, meta_data=None):
    """Adds details of a results file to the output files manifesto.
    """

    global manifest

    valid_types = ['figure', 'text', 'json_data', 'image', 'other_data']
    if file_type not in valid_types:
        raise InvalidOctueFileType('Provided file type "{ft}" is not in the list of valid types. Try one of: {vt}'.format(ft=file_type,vt=valid_types))

    # Is manifest defined yet?
    try:
        manifest
    except NameError:
        manifest = list()

    manifest.append({'file_type': file_type, 'file_name': file_name, 'meta_data': meta_data})
