import json
from json import JSONEncoder, JSONDecoder

from octue.exceptions import InvalidManifestType, InvalidInput
from octue import utils

from .data_file import DataFile


TYPE_REMOTE = 'remote'      # Remote file manifest (files not present on octue)
TYPE_BUILD = 'build'        # Build system manifest (build files only, for octue internal use)
TYPE_DATASET = 'dataset'    # Single-Dataset manifest (files in a dataset)
TYPE_MULTI = 'multi'        # Multi-Dataset manifest (files in multiple datasets)
TYPE_CHOICES = [TYPE_REMOTE, TYPE_BUILD, TYPE_DATASET, TYPE_MULTI]

STATUS_CREATED = 'created'          # Manifest created
STATUS_PROCESSING = 'processing'    # Running automatic manifesting algorithm
STATUS_SUCCESS = 'success'          # Manifesting complete (success)
STATUS_FAILED = 'failed'            # Manifesting complete (failed)
STATUS_CHOICES = [STATUS_CREATED, STATUS_PROCESSING, STATUS_SUCCESS, STATUS_FAILED]


class ManifestEncoder(JSONEncoder):
    """Base encoder for manifests
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
    """ Manifest of files in one or more datasets

    A manifest is used to read a list of files (and their associated properties) into octue analysis, or to compile a
    list of output files (results) and their properties that will be sent back to the octue system.

    """

    uuid = None
    type = None
    files = None

    def __init__(self, **kwargs):
        """Construct a file Manifest
        """
        self.__dict__.update(**kwargs)

        if self.type not in TYPE_CHOICES:
            raise InvalidManifestType(
                'Attempted to specify an invalid manifest type. Valid types: {}'.format(TYPE_CHOICES)
            )

        if self.uuid is None:
            self.uuid = utils.gen_uuid()

        if self.files is None:
            self.files = []

    def append(self, **kwargs):
        """ Add a data/results file to the manifest

        Usage:
            my_file = octue.DataFile(...)
            my_manifest.append(datafile=my_file)

            # or more simply
            my_manifest.append(**{...}) which implicitly creates the datafile from the starred list of input arguments

        TODO allow for appending a list of datafiles
        """
        if 'data_file' in kwargs.keys():
            if kwargs['data_file'].__class__.__name__ != 'DataFile':
                raise InvalidInput(
                    'Object "{}" must be of type DataFile to append it to a manifest'.format(kwargs['data_file'])
                )
            self.files.append(kwargs['datafile'])

        else:
            # Append a single file, constructed by passing the arguments through to DataFile()
            self.files.append(DataFile(**kwargs))

    def get_files(self, method='name_icontains', files=None, filter_value=None):
        """ Get a list of data files in a manifest whose name contains the input string

        TODO improved comprehension for compact search syntax here.
         Searching in different fields, dates date ranges, case sensitivity, search in path, metadata searches,
         filestartswith, search indexing of files, etc etc. Could have a list of tuples with different criteria, AND
         them or OR them.

        :return: results list of matching datafiles
        """

        # Search through the input list of files or by default all files in the manifest
        files = files if files else self.files

        results = []
        for file in files:
            if method == 'name_icontains' and filter_value.lower() in file.name.lower():
                results.append(file)
            if method == 'name_contains' and filter_value in file.name:
                results.append(file)
            if method == 'name_endswith' and file.name.endswith(filter_value):
                results.append(file)
            if method == 'tag_exact' and filter_value in file.tags:
                results.append(file)
            if method == 'tag_startswith':
                for tag in file.tags:
                    if tag.startswith(filter_value):
                        results.append(file)
                        break
            if method == 'tag_endswith':
                for tag in file.tags:
                    if tag.endswith(filter_value):
                        results.append(file)
                        break
            if method == 'in_sequence':
                for tag in file.tags:
                    if tag.startswith('sequence'):
                        results.append(file)
                        break

        return results

    def get_file_sequence(self, filter_value=None, method='name_icontains', files=None):
        """ Get an ordered sequence of files matching a criterion

        Accepts the same search arguments as `get_files`.

        """

        results = self.get_files(filter_value=filter_value, method=method, files=files)
        results = self.get_files(method='in_sequence', files=results)

        # Take second element for sort
        def get_sequence_number(file):
            for tag in file.tags:
                if tag.startswith('sequence'):
                    sequence_number = int(tag.split(':')[1])

        # Sort the results on ascending sequence number
        results.sort(key=get_sequence_number)

    def get_file_by_tag(self, tag_string):
        """ Gets a data file from a manifest by searching for files with the provided tag(s)\

        Gets exclusively one file; if no file or more than one file is found this results in an error.

        :param tag_string: if this string appears as an exact match in the tags
        :return: DataFile object
        """
        results = self.get_files(method='tag_exact', filter_value=tag_string)
        if len(results) > 1:
            raise UnexpectedNumberOfResults('More than one result found when searching for a file by tag')
        elif len(results) == 0:
            raise UnexpectedNumberOfResults('No files found with this tag')

    def save(self, manifest_file_name):
        """ Write a manifest file

        Used either as a utility for locally generating an input manifest (e.g. when testing an app), or to construct
        an output or build manifest during app creation or analyses.

        :param: manifest_file_name the file to write to, including relative or absolute path and .json extension
        """
        json.dump(self, manifest_file_name, cls=ManifestEncoder, sort_keys=True, indent=4)

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
            files = []
            if 'files' in json_object:
                files = [DataFile.deserialise(data_file_json) for data_file_json in json_object.pop('files')]

            return {**json_object, 'files': files}

        return Manifest(**JSONDecoder(object_hook=as_data_file_list).decode(json))

    @staticmethod
    def load(file_name=None):
        """Load manifest from and validate contents of a manifest file
        :return: Instantiated Manifest object
        """
        with open(file_name, 'r') as file:
            return Manifest.deserialise(file.read())


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
    uuid = None
    return uuid
