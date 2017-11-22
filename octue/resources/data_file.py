

class DataFile(object):
    """

    classdef DataFile < handle
        %DATAFILE Handle class for representing data files on the Octue system
        %
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

            % Name is the full path and name of the file on the present system. If
            % the file is created on this system, this name should match the key.
            % However, if the file has been checked out from the Octue platform,
            % this name is likely to be something like:
            %   /input/<uuid>
            Name

            % The original (relative to the root dataset directory) path and name
            % of the file, from when the file was created. This is probably
            % somewhat human-readable, like:
            %   /data/experiment_1/run_0001.csv
            Key

            % The universally unique ID of this file on the Octue system. If the
            % file is created locally this may be empty.
            Uuid

            % Any textual tags associated with the datafile, useful for smart search
            Tags = ''

        end

        methods

            function [obj] = DataFile(varargin)
                %DATAFILE Construct DataFile class object
                %
                %   DATAFILE(s) Initialises a DataFile object from 1x1 structure s,
                %   which must contain fields .uuid, .tag, .name and .key
                %
                %   DATAFILE(name, key, uuid, tags) Initialises a DataFile object
                %   directly from its base properties. Arguments key, uuid and tags
                %   are optional

                if isstruct(varargin{1})
                    % TODO Dynamic property assignment from structure contents
                    obj.Key = varargin{1}.key;
                    obj.Name = varargin{1}.name;
                    obj.Uuid = varargin{1}.uuid;
                    obj.Tags = varargin{1}.tags;
                else
                    % Construct
                    obj.Name = varargin{1};
                    if nargin >= 2
                        obj.Key = varargin{2};
                    else
                        obj.Key = obj.Name;
                    end
                    if nargin >= 3
                        obj.Uuid = varargin{3};
                    end
                    if nargin >= 4
                        obj.Tags = varargin{4};
                    end
                end
                % TODO Other validators (e.g. ischar, disallowed characters)
                if ~exists(obj)
                    error('File does not exist on the current system: %s', obj.Name)
                end

            end

            function obj = addTags(obj, tags)
                %ADDTAGS Adds a string to the object tags, with correct whitespacing
                if ~ischar(tags)
                    % TODO improved client-side validation to ensure input tags are compliant
                    error('tags must be a character string expressing a set of space-delimited octue compliant tags.')
                end
                obj.Tags = [obj.Tags ' ' tags];
            end

            function tf = exists(obj)
                %EXISTS Returns true if the file exists, false otherwise
                tf = false;
                if exist(obj.Name, 'file') == 2
                    tf = true;
                end
            end

            function str = toJson(obj)
               %TOJSON Serialises the data file representation to a json string

               % TODO escape characters - for example the path c:\new will kill
               % this. Use jsonencode() (2017a on) to avoid this agony.

               % Remaining consistent with the MATLAB style guide for object
               % properties prevents us from automatically serialising (for the time
               % being).
               str = sprintf('{"name": "%s", \n "uuid": "%s",\n "key": "%s",\n "tags": "%s"\n}', obj.Name, obj.Uuid, obj.Key, obj.Tags);
            end

        end

    end
    """

    # TODO implement matlab-sdk consistent datafile objects

    uuid = None
    name = None
    key = None
    tags = list()
