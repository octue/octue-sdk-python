import os
import json
from ..exceptions import *
from ..get import get
from ..resources import DataFile
from .. import utils
from json import JSONEncoder, JSONDecoder


class ManifestEncoder(JSONEncoder):
    """Base encoder for manifest files
    """
    def default(self, obj):
        # TODO generalise to if __hasattr__('serialise') and abstract the Encoder class away
        if isinstance(obj, DataFile):
            return obj.serialise()

        # TODO consider using object dict by default
        # return o.__dict__

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class Manifest(object):
    """ MANIFEST Manifest of files
    A manifest is used to read list of files (and their associated properties)
    input to an octue analysis or to compile a list of output files (results)
    and their properties that will be sent back to the octue sytem.

    classdef Manifest < handle
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

    """
        
    uuid = None
    type = 'input'
    files = []

    _appendWarningTriggered = False

    def __init__(self, type='input', json=None):
        """Construct a file manifest object of given type.
        """
            
        if type not in ['input', 'output', 'build', 'cache', 'json']:
            raise InvalidManifestType('Instantiate a Manifest object like man = Manifest(type) where type is a string which can have the values ''input'' or ''output''.')

        # Create a manifest UUID. This is overwritten if an existing manifest is deserialised or loaded.
        self.uuid = utils.gen_uuid()

        self.type = type

        if self.type == 'cache':
            raise NotImplementedYet('cache type file manifests are unused by octue at present')

                # case 'json'
                #     if (nargin ~= 2)
                #         error('Constructing a manifest from a json string requires two inputs: try Manifest(''json'', json_str).')
                #     elseif ~ischar(json)
                #         error('Input json not a string')
                #     elseif isempty(json)
                #         error('Input json string is empty')
                #     end
                #     self = self.Deserialise(json);
                #     return


        if (self.type != 'json') and (json is not None):
            raise InvalidInput('Manifest was created with "input", "output", "build" or "cache" type, but additional json argument is given. Use "json" to create a manifest from a json string.')

    def append(self, **kwargs):
        """APPEND Adds a results file to the output manifest
        """

        # TODO issue a non-recurring warning for appending files to an input manifest
        # The matlab:
        #     if ~(strcmpi(self.Type, 'output') || strcmpi(self.Type, 'build'))
        #         if ~self.AppendWarningTriggered
        #             warning('Do not append files to an input manifest.')
        #             self.AppendWarningTriggered = true;
        #         end
        #     end

        if 'datafile' in kwargs.keys():

            # TODO allow for appending a list of datafiles
            # TODO check it's actually a datafile class (Matlab: if isa(varargin{1}, 'octue.DataFile'))
            self.files.append(kwargs['datafile'])

        else:
            # Append a single file, constructed by passing the arguments through to DataFile()
            self.files.append(DataFile(**kwargs))

    def get_file_by_tag(self, tag_string):
        """ Gets a filename from a manifest by searching for
            %files with the provided tag(s). Gets exclusively one file; if no file
            %or more than one file is found this results in an error.

        :param tag_string:
        :return: DataFile object
        """

        # TODO
        # % Split the input tagset into discrete tags
        # tags = strsplit(tagString);
        #
        # % Fill a logical matrix where individual tags are found in files
        # nTags = numel(tags);
        # nFiles = numel(self.Files);
        # found = false([nTags nFiles]);
        #
        # for iTag = 1:nTags
        #     for iFile = 1:nFiles
        #         k = strfind(self.Files(iFile).Tags, tags{iTag});
        #         if ~isempty(k)
        #             found(iTag, nFiles) = true;
        #         end
        #     end
        # end
        #
        # % Check for files where all tags were found. Error on zero or
        # % multiple files
        # found = all(found, 1);
        # if sum(found) == 0
        #     error(['No files found in the manifest with tags ''' tagString '''.'])
        # elseif sum(found) > 1
        #     error(['More than one file found in the manifest with tags ''' tagString '''.'])
        # end
        #
        # % Output the one file for which all tags were found
        # file = self.Files(found);
        raise NotImplementedYet()

    def save(self, manifest_file=None):
        """ Write a manifest file
        Used either as a utility for locally generating an input manifest (e.g. when testing an app), or to construct
        an output or build manifest during app creation or analyses.
        """

        # If no name passed, save in input or output directory based on type (or current dir by default)
        if not manifest_file:
            if self.type == 'input':
                manifest_file = os.path.join(get('InputDir'), 'manifest.json')
            elif self.type == 'output':
                manifest_file = os.path.join(get('OutputDir'), 'manifest.json')
            else:
                manifest_file = 'manifest.json'

        json.dump(self, manifest_file, cls=ManifestEncoder, sort_keys=True, indent=4)

    def serialise(self):
        """ Serialises this manifest into a json string
        """
        return json.dumps(self, cls=ManifestEncoder)

    @staticmethod
    def deserialise(json):
        """ Initialises a manifest using the contents of a json string. Note snake_case convention in the manifest and
        config files is consistent with the PEP8 style used here, so no need for name conversion.
        """

        def as_data_file_list(json_object):
            if 'files' in json_object:
                return [DataFile.deserialise(data_file_json) for data_file_json in json_object['files']]
            return json_object

        # TODO validations
        return JSONDecoder(object_hook=as_data_file_list).decode(json)

    @staticmethod
    def load(manifest_file=None):
        """Load manifest from and validate contents of a manifest file
        :return: Instantiated Manifest object
        """

        # If no name passed, load from current directory
        if not manifest_file:
            manifest_file = 'manifest.json'

        return Manifest('file', manifest_file)


# def add_to_manifest(file_type, file_name, meta_data=None):
#     """Adds details of a results file to the output files manifest
#     """
#
#     global manifest
#
#     # Is manifest defined yet?
#     try:
#         manifest
#     except NameError:
#         manifest = list()


def add_figure(**kwargs):
    """ Adds a figure to the output file manifest. Automatically adds the tags 'type:fig extension:json'
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
