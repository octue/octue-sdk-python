import logging

from octue.exceptions import InvalidInputException, ProtectedAttributeException
from octue.mixins import Identifiable, Loggable, Serialisable, Taggable
from octue.resources.manifest import Manifest
from twined import ALL_STRANDS, Twine


module_logger = logging.getLogger(__name__)


# Maps the kind of strand we're using to the class which Twined will instantiate
CLASS_MAP = {"manifest": Manifest}


class Analysis(Identifiable, Loggable, Serialisable, Taggable):
    """ Analysis class, holding references to all input and output data

    ## The Analysis Instance

    An Analysis instance is unique to a specific computation analysis task, however large or small, run at a specific
    time. It will be created by the task runner using the raw input data.

    It holds references to all config, input and output data, logs, connections to child twins, credentials, etc, so
    should be referred to from your code to get those items.

    It's basically the "Internal API" for your data service - a single point of contact where you can get or update
    anything you need.

    Analyses are instantiated at the top level of your app/service/twin code and you can import the instantiated
    object from there (see the templates for examples)

    ## WHAT DO I NEED

    - update(self, status)
        mark analysis as having progressed with a status (e.g. complete) and a reason.
        Execute a callback on update.
        %COMPLETE Should be called upon completion of the analysis
        % Completes the analysis by:
        %   - saving the output manifest to a json file
        %   - updating progress indicators

        % TODO add status information
        outputManifestFile = fullfile('outputs', 'manifest.json');
        self.OutputManifest.Save(outputManifestFile)

    - getattr
        - configuration_values
        - configuration_manifest
        - input_values
        - input_manifest
        - credentials
        - children
        - monitors
        - logger

    - serialise(self)

    """

    def __init__(self, twine, **kwargs):
        """ Constructor of Analysis instance
        """

        # Instantiate the twine (if not already) and attach it to self
        if not isinstance(twine, Twine):
            twine = Twine(source=twine)

        self.twine = twine

        # Pop any possible strand data sources before instantiating the superclasses
        strand_data_kwargs = dict((name, kwargs.pop(name, None)) for name in ALL_STRANDS)
        super().__init__(**kwargs)

        # For each possible strand passed as a kwarg, validate against the twine and instantiate
        for name in self.twine.available_strands:
            data = strand_data_kwargs.pop(name)  # Name will be in strand_data_kwargs, but its value may be None
            if data is None:
                raise InvalidInputException(
                    f"The {name} strand is defined in the twine, but no data is provided to Analysis()"
                )

            self.__setattr__(f"_{name}", self._manufacture(name, data))

        # If and data are passed against strands not in the twine...
        for name, data in strand_data_kwargs.items():
            if data is not None:
                raise InvalidInputException(f"Data is provided for {name} but no such strand is defined in the twine")

    def __setattr__(self, name, value):
        """ Override setters for protected attributes (the strand contents may change, but the strands themselves
        shouldn't be changed after instantiation)
        """
        if name in ALL_STRANDS:
            raise ProtectedAttributeException(f"You cannot set {name} on an instantiated Analysis")

        super().__setattr__(name, value)

    def __getattr__(self, name):
        """ Override public getters to point to protected attributes (the strand contents may change, but the strands
        themselves shouldn't be changed after instantiation)
        """
        if name in ALL_STRANDS:
            return getattr(self, f"_{name}")

    def _manufacture(self, name, value):
        """ Uses Twined to validate data and instantiate it to a custom class
        """
        cls_key = name.split("_")[-1]

        # Custom classes which we want Twined to instantiate for us
        cls = CLASS_MAP.get(cls_key, None)

        # Use Twine to validate and instantiate as the desired class
        if not isinstance(value, type(cls)):
            self.logger.debug(
                "Instantiating %s as %s and validating against twine", name, cls.__name__ if cls else "default_class"
            )
            return self.twine.validate(name, source=value, cls=cls)

        return value
