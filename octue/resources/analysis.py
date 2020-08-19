import json
import logging
import os

from octue.exceptions import FolderNotPresent
from .manifest import Manifest


class Analysis(object):
    """ Analysis configuration for running an app

    The Analysis class provides a set of configuration parameters for use by
    your application, together with a range of methods for managing input and
    output file parsing as well as controlling logging.

    A single analysis object should exist, passed by reference so any additions or modifications made by one routine
    are accessible wherever else the instance is used. This can be used for communication between functions, but such
    usage is not recommended: modification or subclassing of the Analysis object should be treated with extreme caution

    TODO implement the following remaining methods for consistency with the MATLAB SDK

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
        %serialised to disk, to avoid config parameters being saved.
        error('Attempted to save Analysis class object. This should absolutely be avoided, since security settings from
        the environment (e.g. API keys for third party services) may be attached to Analysis.Config.')
    end

    function SetLocal(self)
        %SETLOCAL Sets the flag for a local analysis, which is used for
        % (among other things) determining whether to attempt to plot
        % figures or not.
        self.IsLocal = true;
    end

    """

    id = None
    input_dir = None
    log_dir = None
    tmp_dir = None
    output_dir = None
    input_manifest = None
    output_manifest = None
    config = None
    logger = None

    @property
    def is_local(self):
        """ True if local analysis is being run (i.e. no id present)
        :return:
        """
        return ~(self.id is None)

    def setup(
        self, id=None, data_dir=".", input_dir=None, log_dir=None, output_dir=None, tmp_dir=None, skip_checks=False
    ):
        """ Sets up the analysis object
        :param id:
        :param data_dir:
        :param input_dir:
        :param log_dir:
        :param output_dir:
        :param tmp_dir:
        :param skip_checks:
        :return:
        """
        self.input_dir = input_dir if input_dir else data_dir + "/input"
        self.log_dir = log_dir if log_dir else data_dir + "/log"
        self.output_dir = output_dir if output_dir else data_dir + "/output"
        self.tmp_dir = tmp_dir if tmp_dir else data_dir + "/tmp"

        if not os.path.isdir(self.input_dir):
            raise FolderNotPresent("Missing input directory: {}".format(self.input_dir))
        if not os.path.isdir(self.log_dir):
            raise FolderNotPresent("Missing log directory: {}".format(self.log_dir))
        if not os.path.isdir(self.output_dir):
            raise FolderNotPresent("Missing output directory: {}".format(self.output_dir))
        if not os.path.isdir(self.tmp_dir):
            raise FolderNotPresent("Missing tmp directory: {}".format(self.tmp_dir))

        # Attach configuration properties to the analysis object
        self.config_from_file(skip_checks)

        # Initialise the loggers, with unified formatting
        self.init_log()

        # Create input and output manifests
        self.input_manifest_from_file(skip_checks)
        self.output_manifest = Manifest(type="dataset")

    def config_from_file(self, skip_checks=False):
        """ Read the config.json file into a dict
        :param skip_checks: If true, skip the validation of the read-in object against the application schema
        :return: None
        """

        config_file_name = str(os.path.join(self.input_dir, "config.json"))
        with open(config_file_name, "r") as config_file:
            self.config = json.load(config_file)

        if ~skip_checks:
            # TODO validate the config against the schema!!!
            pass

    def init_log(self):

        """ Configures application level console logging options
        :return:
        """

        # TODO allow varied log level
        logging.basicConfig(
            format="%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger("octue")

        # TODO add file handlers and octue-specific and app-specific loggers.
        #  See https://docs.python.org/3/howto/logging.html#logging-basic-tutorial  to understand the logger hierarchy
        # octue_logger = logging.getLogger('octue')
        # app_logger = logging.getLogger('app')
        # print('__NAME__', __NAME__)
        #
        # # Create console handler and set level to info
        # ch = logging.StreamHandler()
        # ch.setLevel(logging.INFO)
        #
        # # create formatter
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        #
        # # add formatter to ch
        # ch.setFormatter(formatter)
        #
        # # add ch to logger
        # logger.addHandler(ch)

    def input_manifest_from_file(self, skip_checks=False):

        input_manifest_file = os.path.join(self.input_dir, "manifest.json")
        self.logger.info("Loading from manifest file: %s", input_manifest_file)
        self.input_manifest = Manifest.load(input_manifest_file)


# Instantiate so a single analysis instance is referred to by all code
analysis = Analysis()
