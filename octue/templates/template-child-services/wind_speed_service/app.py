import random


def run(analysis):
    """Run a mock analysis that returns a random integer wind speed for each given location.

    :param octue.resources.analysis.Analysis analysis:
    :return None:
    """
    analysis.output_values = [random.randint(0, 200) for location in analysis.input_values["locations"]]
    analysis.finalise()
