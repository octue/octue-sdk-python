import random


def run(analysis):
    """Run a mock analysis producing random integers as elevations to send to the parent.

    :param octue.resources.Analysis analysis:
    :return None:
    """
    analysis.output_values = [random.randint(0, 5000) for location in analysis.input_values["locations"]]
    analysis.finalise()
