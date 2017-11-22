import json
from .. import utils


class DataFile(object):
    """ Class for representing data files on the Octue system
    """

    # name is the full path and name of the file on the present system. If the file is created on this system, this
    # name should match the key.
    # However, if the file has been checked out from the Octue platform, this name is likely to be something like:
    #   /input/<uuid>
    name = None

    # The original (relative to the root dataset directory) path and name of the file, from when the file was created.
    # This is probably somewhat human-readable, like:
    #   /data/experiment_1/run_0001.csv
    key = None

    # The universally unique ID of this file on the Octue system. If the
    # file is created locally this may be empty.
    uuid = None

    # Any textual tags associated with the datafile, useful for smart search
    tags = list()

    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def serialise(self):
        """ Serialises this object into a json string
        """
        return json.dumps(self)

    @staticmethod
    def deserialise(json_str):
        """ Initialises a DataFile using the contents of a json string. Note snake_case convention in the manifest and
        config files is consistent with the PEP8 style used here, so no need for name conversion.
        """
        # TODO validations
        return DataFile(**json.loads(json_str))

    def addTags(self, tags):
        """ Adds a new tag string to the object tags, with correct whitespacing
        """
        # TODO improved client - side validation to ensure input tags are compliant error('tags must be a character string expressing a set of space-delimited octue compliant tags.')
        if isinstance(tags, list):
            self.tags += tags
        else:
            self.tags.append(tags)

    def exists(self):
        """ Returns true if the datafile exists on the current system, false otherwise
        Here mostly for consistency with the MATLAB SDK
        :return: bool
        """
        return utils.isfile(self.name)