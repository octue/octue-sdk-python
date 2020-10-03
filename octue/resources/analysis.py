import logging

from octue.exceptions import ProtectedAttributeException
from octue.mixins import Identifiable, Loggable, Serialisable, Taggable
from octue.resources.manifest import Manifest
from twined import ALL_STRANDS, Twine


module_logger = logging.getLogger(__name__)


# Map strand names to class which we expect Twined to instantiate for us
CLASS_MAP = {"configuration_manifest": Manifest, "input_manifest": Manifest, "output_manifest": Manifest}


class Analysis(Identifiable, Loggable, Serialisable, Taggable):
    """ Analysis class, holding references to all input and output data

    ## The Analysis Instance

    An Analysis instance is unique to a specific computation analysis task, however large or small, run at a specific
    time. It will be created by the task runner (which will have validated incoming data already - Analysis() doesn't
    do any validation).

    It holds references to all config, input and output data, logs, connections to child twins, credentials, etc, so
    should be referred to from your code to get those items.

    It's basically the "Internal API" for your data service - a single point of contact where you can get or update
    anything you need.

    Analyses are instantiated at the top level of your app/service/twin code and you can import the instantiated
    object from there (see the templates for examples)

    :parameter twine: Twine instance or json source
    :parameter configuration_values: see Runner.run() for definition
    :parameter configuration_manifest: see Runner.run() for definition
    :parameter input_values: see Runner.run() for definition
    :parameter input_manifest: see Runner.run() for definition
    :parameter credentials: see Runner.run() for definition
    :parameter monitors: see Runner.run() for definition
    :parameter output_values: see Runner.run() for definition
    :parameter output_manifest: see Runner.run() for definition
    :parameter id: Optional UUID for the analysis
    :parameter logger: Optional logging.Logger instance attached to the analysis
    """

    def __init__(self, twine, **kwargs):
        """ Constructor of Analysis instance
        """

        # Instantiate the twine (if not already) and attach it to self
        if not isinstance(twine, Twine):
            twine = Twine(source=twine)

        self.twine = twine

        # Pop any possible strand data sources before init superclasses (and tie them to protected attributes)
        strand_kwargs = dict((name, kwargs.pop(name, None)) for name in ALL_STRANDS)
        for strand_name, strand_data in strand_kwargs.items():
            self.__setattr__(f"_{strand_name}", strand_data)

        # Init superclasses
        super().__init__(**kwargs)

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
