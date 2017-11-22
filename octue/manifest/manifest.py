from ..exceptions import InvalidOctueFileType


class Manifest(object):
    """
    TODO implement consistent to MATLAB
    classdef Manifest < handle
    %MANIFEST Manifest of files
    % A manifest is used to read list of files (and their associated properties)
    % input to an octue analysis or to compile a list of output files (results)
    % and their properties that will be sent back to the octue sytem.
    %
    % Manifest Properties:
    %   Type - A string, 'input' for input (read-only) manifests and
    %   'output' for appendable output manifests
    %   Files - An array of DataFile objects, each referencing a file and its
    %   associated metadata
    %
    % Manifest Methods:
    %   Manifest - Instantiate an object of class Manifest
    %   Append - Append a file or files to the manifest
    %   Save - Serialise the manifest to a json type file
    %   Load - Deserialise a manifest from a json type file
    properties(SetAccess = 'protected')
        
        Uuid
        Type = 'input'
        Files = []
    
    end
    
    properties (Access = 'private')
        AppendWarningTriggered = false
    end
    
    methods
        
        function self = Manifest(type, json)
            %MANIFEST Construct a file manifest object of given type.
            
            if ~ischar(type)
                error('Instantiate a Manifest object like man = Manifest(type) where type is a string which can have the values ''input'' or ''output''.')
            end
            
            % Create a UUID. This is overwritten if an existing manifest is
            % deserialised or loaded.
            self.Uuid = octue.utils.genUUID();
            
            switch lower(type)
                case 'input' 
                    self.Type = 'input';
                    
                case 'output'
                    self.Type = 'output';
                    
                case 'cache'
                    warning('cache type file manifests are unused by octue at present')
                    
                case 'build'
                    self.Type = 'build';
                    
                case 'json'
                    if (nargin ~= 2) 
                        error('Constructing a manifest from a json string requires two inputs: try Manifest(''json'', json_str).')
                    elseif ~ischar(json)
                        error('Input json not a string')
                    elseif isempty(json)
                        error('Input json string is empty')
                    end
                    self = self.Deserialise(json);
                    return
                    
                otherwise
                    error('Unknown manifest type specified. Try ''input'' or ''output''.')
                    
            end
            
            if nargin ~= 1 
                warning('Manifest was created with ''input'', ''output'', ''build'' or ''cache'' type, additional argument is ignored')
            end
            
        end
        
        function Append(self, varargin)
            %APPEND Adds a results file to the output manifest
            
            if ~(strcmpi(self.Type, 'output') || strcmpi(self.Type, 'build'))
                if ~self.AppendWarningTriggered
                    warning('Do not append files to an input manifest.')
                    self.AppendWarningTriggered = true;
                end
            end
            
            if isa(varargin{1}, 'octue.DataFile')
                % Append any number of file object arguments to the output manifest
                for iArg = 1:nargin-1
                    self.Files = [self.Files; varargin{iArg}];
                end
            else
                % Append a single file, constructed by passing the arguments
                % through to the output file object constructor
                self.Files = [self.Files; octue.DataFile(varargin{:})];
            end
            
        end
        
        function Save(self, manifestFile)
            %SAVE Write a manifest file
            % Used either as a utility for locally generating an input manifest
            % (e.g. when testing an app), or to construct an output or build 
            % manifest.
            
            % If no name passed, save in input or output directory based on
            % type (or current dir by default)
            if nargin == 1
                switch self.Type
                    case 'input'
                        manifestFile = fullfile(octue.get('InputDir'), 'manifest.json');
                    case 'output'
                        manifestFile = fullfile(octue.get('OutputDir'), 'manifest.json');
                    otherwise
                        manifestFile = 'manifest.json';
                end
            end
        
            % Open a file for writing text (discarding existing contents if any), print to it, close it
            fileId = fopen(manifestFile, 'wt');
            nBytes = fprintf(fileId, '%s', Serialise(self));
            status = fclose(fileId);
            assert(status==0, ['Unable to write manifest file: ' manifestFile])
            fprintf('Wrote %i bytes to file %s\n', nBytes, manifestFile)
            
        end
        
        function file = GetFileByTag(self, tagString)
            %GETFILEBYTAG Gets a filename from a manifest by searching for
            %files with the provided tag(s). Gets exclusively one file; if no file
            %or more than one file is found this results in an error.
            
            % Split the input tagset into discrete tags
            tags = strsplit(tagString);
            
            % Fill a logical matrix where individual tags are found in files
            nTags = numel(tags);
            nFiles = numel(self.Files);
            found = false([nTags nFiles]);
            
            for iTag = 1:nTags
                for iFile = 1:nFiles
                    k = strfind(self.Files(iFile).Tags, tags{iTag});
                    if ~isempty(k)
                        found(iTag, nFiles) = true;
                    end
                end
            end
            
            % Check for files where all tags were found. Error on zero or
            % multiple files
            found = all(found, 1);
            if sum(found) == 0
                error(['No files found in the manifest with tags ''' tagString '''.'])
            elseif sum(found) > 1
                error(['More than one file found in the manifest with tags ''' tagString '''.'])
            end
            
            % Output the one file for which all tags were found
            file = self.Files(found);
            
        end
        
        function str = Serialise(self)
            %SERIALISE Serialises a manifest into a json string
            
            % TODO improved serialisation by passing MATLAB structures into
            % jsonencode() (R2016b and later)
            
            % TODO Update toJson method in octue.DataFile to match the
            % Serialise() api here.
            
            % Serialise each file into a list
            files = '[';
            for i = 1:numel(self.Files)-1
                files = [files self.Files(i).toJson() ', ']; %#ok<AGROW>
            end
            files = [files self.Files(end).toJson() ']'];
            
            % Create the manifest string
            str = sprintf('{"uuid": "%s", "type": "%s", "files": %s\n}',...
                             self.Uuid, self.Type, files);
        
        end
        
        function self = Deserialise(self, json)
            %DESERIALISE Initialises an manifest using the contents of a json
            %string. Manages conversion from snake_case to CapCamelCase to
            %comply with MATLAB style-guide-conformant classes.
            
            s = jsondecode(json);
            
            % Validate the input json
            % TODO More validators on manifest contents
            if ~ischar(json)
                error('Input json must be a string')
            end
            if ~isfield(s, 'type')
                error('Octue:InvalidManifest', 'No type key in the manifest json: %s', json)
            end
            if ~isfield(s, 'uuid')
                error('Octue:InvalidManifest', 'No uuid key in the manifest json: %s', json)
            end
            if ~isfield(s, 'files')
                error('Octue:InvalidManifest', 'No files key in the manifest json: %s', json)
            end
            
            % Set the manifest properties, initialising a DataFile object array
            self.Type = s.type;
            self.Uuid = s.uuid;
            self.Files = [];
            for i = 1:numel(s.files)
                % TODO remove cell references for R2016b onward
                a = s.files{i};
                self.Files = [self.Files, octue.DataFile(a)];
            end
        end
        
        
    end
    
    methods (Static)
            
        function obj = Load(manifestFile)
            %LOAD Load from and validate contents of a manifest file
            
            % If no name passed, load from current directory
            if nargin > 1
                manifestFile = 'manifest.json';
            end
            
            % Get the json string from the file
            json = fileread(manifestFile);
            
            % Construct a new Manifest object from the json
            obj = octue.Manifest('json', json);
        end
        
    end
    
end
"""


def add_to_manifest(file_type, file_name, meta_data=None):
    """Adds details of a results file to the output files manifest
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


def add_figure(**kwargs):
    """
    %ADDFIGURE Adds a figure to the output file manifest. Automatically adds the
    %tags 'type:fig extension:json'
    %
    %   ADDFIGURE(p) writes a JSON file from a plotlyfig object p (see
    %   figure.m example file in octue-app-matlab).
    %
    %   ADDFIGURE(data, layout) writes a JSON file from data and layout
    %   structures, which must be compliant with plotly spec, using MATLAB's native
    %   json encoder (2017a and later).
    %
    %   ADDFIGURE(..., tags) adds a string of tags to the figure to help the
    %   intelligence system find it. These are appended to the automatically added
    %   tags identifying it as a file.
    %
    %   uuid = ADDFIGURE(...) Returns the uuid string of the created figure,
    %   allowing you to find and refer to it from anywhere (e.g. report templates or
    %   in hyperlinks to sharable figures).

    % Generate a unique filename and default tags
    % TODO generate on the octue api so that the figure can be trivially registered
    % in the DB and rendered
    uuid = octue.utils.genUUID;
    key = [uuid '.json'];
    name = fullfile(octue.get('OutputDir'), [uuid '.json']);
    tags = 'type:fig extension:json ';

    % Parse arguments, appending tags and generating json
    % TODO validate inputs, parse more elegantly, and accept cases where the data
    % and layout keys are part of the structure or not.
    if nargin == 1
        str = plotly_json(varargin{1});

    elseif (nargin == 2) && (isstruct(varargin{2}))
        data = varargin{1};
        layout = varargin{2};
        str = jsonencode({data, layout});

    elseif (nargin == 2)
        str = plotly_json(varargin{1});
        tags = [tags varargin{2}];

    elseif nargin == 3
        data = varargin{1};
        layout = varargin{2};
        str = jsonencode({data, layout});
        tags = [tags varargin{3}];

    end

    % Write the file
    fid = fopen(name, 'w+');
    fprintf(fid, '%s', str);
    fclose(fid);

    % Append it to the output manifest
    file = octue.DataFile(name, key, uuid, tags);
    octue.get('OutputManifest').Append(file)

    end

    function str = plotly_json(p)
    %PLOTLY_JSON extracts json data from a plotlyfig object.

    jdata = m2json(p.data);
    jlayout = m2json(p.layout);
    str = sprintf('{"data": %s, "layout": %s}', escapechars(jdata), escapechars(jlayout));

    end
    """
    uuid=None
    return uuid
