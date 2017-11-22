class Analysis(object):
    """
classdef Analysis < handle
    %ANALYSIS Handle class for accessing analysis configuration from your app.
    %
    % The Analysis class provides a set of configuration parameters for use by
    % your application, together with a range of methods for managin input and
    % output file parsing as well as controlling logging.
    %
    % Being a handle (rather than value) based class, instances are passed
    % around by reference, so any additions or modifications made by one routine
    % are accessible wherever else the instance is used. This can be used for
    % communication between functions, but this usage is not recommended:
    % modification or subclassing of the Analysis object should be treated with
    % extreme caution.
    %
    % Author:                   T. H. Clark
    % Work address:             Ocean Array Systems Ltd
    %                           Hauser Forum
    %                           3 Charles Babbage Road
    %                           Cambridge
    %                           CB3 0GT
    % Email:                    tom.clark@oceanarraysystems.com
    % Website:                  www.oceanarraysystems.com
    %
    % Copyright (c) 2017 Ocean Array Systems Ltd, All Rights Reserved.

    properties (SetAccess = 'protected')

        InputDir = './input'
        LogDir = './log'
        TmpDir = './tmp'
        OutputDir ='./output'
        InputManifest
        OutputManifest
        Config
        Logger

    end

    methods

        function self = Analysis(s)
            %ANALYSIS Construct Analysis class object

            % Attach parsed command line arguments containing folder paths
            if nargin == 1
                if isstruct(s) && numel(s) == 1
                    for prop = {'InputDir','OutputDir','LogDir','TmpDir'}
                        self.(prop{1}) = s.(prop{1});
                    end
                else
                    error('Analysis class may be instantiated with no arguments or with a 1x1 structure s specifying the runtime folders.')
                end
            end

            % Check that folders exist; they're required for an analysis
            if ~octue.utils.isfolder(self.InputDir);  error(['Input folder does not exist: '  self.InputDir]);  end
            if ~octue.utils.isfolder(self.OutputDir); error(['Output folder does not exist: ' self.OutputDir]); end
            if ~octue.utils.isfolder(self.TmpDir);    error(['Tmp folder does not exist: '    self.TmpDir]);    end
            if ~octue.utils.isfolder(self.LogDir);    error(['Log folder does not exist: '    self.LogDir]);    end

            % Attach configuration properties to the analysis object
            self.ConfigFromFile(fullfile(self.InputDir, 'config.json'));

            % Create an input manifest from the file
            self.InputManifest = octue.Manifest.Load(fullfile(self.InputDir, 'manifest.json'));

            % Create a blank output manifest
            self.OutputManifest = octue.Manifest('output');

            % Start logging
            self.InitLog()

            % Register the object as the current analysis, so it's retrievable
            % from anywhere
            octue.get('analysis', self)

        end

        function InitLog(self)
            %INITLOG Start the application logging

            % Get or create a logger named 'octue'
            logger = logging.getLogger('octue', 'path', fullfile(self.LogDir, 'log.txt'));

            % TODO more flexible logging based on the configuration
            logger.setLogLevel(logging.logging.DEBUG)
            self.Logger = logger;

        end

        function ConfigFromFile(self, configFile)
            %CONFIGFROMFILE Read configuration file and dynamically attach
            % readonly config properties to the Analysis object instance

            % Get the json string from the file and convert to a structure,
            % which should be 1x1

            % TODO update to 2016b or later for jsonencode. Currently requires
            % the jsonlab toolbox
%             s = jsondecode(fileread(configFile));
            s = loadjson(configFile);

            if ~isstruct(s) || (numel(s) ~= 1)
                error('Config file should be readable into a 1 x 1 MATLAB structure, i.e. config.json should look like ''{"Field1":1, "Field2": {"Field2a":1,"Field2b":1}}''')
            end

            % For fields in json, dynamically insert the configuration property
            % into the Analysis object
            names = fieldnames(s);
            for iField = 1:numel(names)
                self.Config.(names{iField}) = s.(names{iField});
            end
        end

        function ConfigFromEnvironment(self, varargin)
            %CONFIGFROMENVIRONMENT Dynamically attach properties from named
            % environment variables to the readonly config properties.
            % If the specified environment variables are not present, a warning
            % is issued.

            for iArg = 1:nargin-1

                % For each environment variable name input
                envVarName = varargin{iArg};

                % Check the name is valid
                if ~ischar(envVarName)
                    error('Input environment variable names must be character strings')
                end
                % TODO validate envvarname using regexp to make sure it's a valid fieldname

                % Get the value from the environment and add it to the
                % configuation
                self.Config.(envVarName) = getenv(envVarName);

            end

        end

        function Complete(self)
            %COMPLETE Should be called upon completion of the analysis
            % Completes the analysis by:
            %   - saving the output manifest to a json file
            %   - updating progress indicators

            % TODO add status information
            outputManifestFile = fullfile('outputs', 'manifest.json');
            self.OutputManifest.Save(outputManifestFile)

        end

        function saveobj(self) %#ok<MANU>
            %SAVEOBJ Catches the possibility that an Analysis class is
            %serialised to disk, to avoid config paramters being saved.
            error('Attempted to save Analysis class object. This should absolutely be avoided, since security settings from the environment (e.g. API keys for third party services) may be attached to Analysis.Config.')
        end
    end
end
"""
    #  TODO Implement consistent with the MATLAB SDK

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


