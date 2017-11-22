from .resources import Analysis, DataFile
from .exceptions import InvalidOctueFileType
import click




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



# TODO Check these options match the MATLAB SDK command line argument structure:
"""
function [command, args] = argParser(varargin)
%ARGPARSER Parses the Octue unix command line options
% MATLAB style argument parsing (using inputParser) is recommended within your
% MATLAB codebase, but is a nightmare when it comes to parsing the widely used 
% unix style command line options (which are used by the octue executable
% launcher). 
%
% Here, we do a quick and dirty parse of the input string argument to mimic a
% unix style parser for our CLI options. You shouldn't need to touch this.

% TODO implement a proper unix options parser class

% Defaults
args.Id = [];
args.Local = true;
args.InputDir = './input';
args.OutputDir = './output';
args.TmpDir = './tmp';
args.LogDir = './log';
command = [];

for iArg = 1:numel(varargin)
    switch varargin{iArg}
        case '--id'
            args.Id = varargin{iArg+1};
        case '--local'
            args.Local = varargin{iArg+1};
        case '--verbose'
            warning('Found deprecated verbose option: control output verbosity using log level.')
        case '--input-dir'
            args.InputDir = varargin{iArg+1};
        case '--tmp-dir'
            args.TmpDir = varargin{iArg+1};
        case '--output-dir'
            args.OutputDir = varargin{iArg+1};
        case '--log-dir'
            args.LogDir = varargin{iArg+1};
        case 'run'
            if ~isempty(command)
                error('You cannot specify multiple commands. Use either ''run, ''version'' or ''schema''.')
            end
            command = 'run';
        case 'version'
            if ~isempty(command)
                error('You cannot specify multiple commands. Use either ''run, ''version'' or ''schema''.')
            end
            command = 'version';
        case 'schema'
            if ~isempty(command)
                error('You cannot specify multiple commands. Use either ''run, ''version'' or ''schema''.')
            end
            command = 'schema';
        case 'mapfile'
            warning('Found deprecated mapfile command: using <inputDir>/manifest.json and <inputDir>/config.json')
    end
end

% Check for the required argument
if isempty(command)
    error('You must specify a command. ''run'', ''version'' and ''schema'' are valid options.')
end

end
"""