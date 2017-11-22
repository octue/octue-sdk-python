from .exceptions import NotImplementedYet


def local(data_dir, command):
    """
    %LOCAL Creates and registers a local Octue analysis locally.
    %
    % Note: local() will not register the analysis or its results on the Octue
    % system (i.e. remote processing), it is for local app development only.
    %
    % For remote processing you need to contact support@octue.com.
    %
    %   LOCAL() Launches a local analysis on data residing in /input, /tmp, /log,
    %   /output directories, which must exist in the current working dir.
    %
    %   LOCAL(dataDir) Launches a local analysis on data residing in /input, /tmp, 
    %   /log, /output directories in the dataDir directory.
    %
    %   LOCAL(dataDir, '--force-reset') Launches a local analysis on data residing 
    %   in /input, /tmp, /log, /output directories in the dataDir directory,
    %   clearing any existing analysis first. Useful for rapid development, but will
    %   clear manifests and caches.
    %   
    % Once the analysis is registered, you can proceed with running your application
    % by calling it just as you do from the octue example app wrapper.
    
    % Set default behaviour
    if nargin <= 0
        command = '--no-force-reset';
    end
    
    % Default: Use input/, tmp/, output/ folders inside current working directory
    if nargin == 0
        dataDir = pwd;
        
        % TODO checking method to prevent wet code like this
        if ~octue.isfolder('input')
            error('./input folder not found. Either specify a data directory as local(''path/To/Data/Dir'') or move to the data directory location to start a local analysis.')
        end
        
    end
    
    if strcmpi(command,'--force-reset')
        warning('Invoked --force-reset on octue.local(): Clearing existing analysis.')
        octue.clear()
    end
        
    % If there's a createManifest.m file in the input/ directory, use it to create
    % the input manifest file. Otherwise assume that manifest.json is present in the
    % input directory.
    
    % TODO Move this into the Analysis constructor, to avoid a circular dependency 
    % if createManifest.m requires the Analysis object or configuration parameter
    if ~octue.utils.isfile(dataDir, 'input', 'manifest.json')
        warning('no input manifest.json file present. Attempting to create manifest: %s', fullfile(dataDir, 'input', 'manifest.json'))
        if octue.utils.isfile(dataDir, 'input', 'createManifest.m')
            current_dir = pwd;
            try
                inputDir = fullfile(dataDir, 'input');
                % Ensures that multiple createManifest scripts don't conflict by
                % using the one in the input directory only
                cd(inputDir)
                createManifest(inputDir)
            catch me
                cd(current_dir)
                rethrow(me)
            end
            % Move back to the original directory
            cd(current_dir)
        else
            error('Unable to find or create input manifest file. Ensure that a manifest.json file is present in your /input data directory.')
        end
    end
    
    
    % Construct and register analysis object using a structure to initialise
    s.InputDir = fullfile(dataDir, 'input');
    s.OutputDir = fullfile(dataDir, 'output');
    s.LogDir = fullfile(dataDir, 'log');
    s.TmpDir = fullfile(dataDir, 'tmp');
    octue.Analysis(s);
    
    % Clear prior results
    % TODO full clearup
    % if strcmpi(command,'--force-reset')
    %     warning('Invoked --force-reset on octue.local(): Flushing caches and removing prior output files.')
    %     octue.removeFiles()
    % end
    """

    raise NotImplementedYet()
