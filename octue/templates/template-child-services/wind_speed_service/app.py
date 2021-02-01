import random


def run(analysis):
    analysis.output_values = [random.randint(0, 200) for location in analysis.input_values["locations"]]
    analysis.finalise()
