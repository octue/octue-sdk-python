import random


def run(analysis):
    analysis.output_values = [random.randint(0, 5000) for location in analysis.input_values["locations"]]
    analysis.finalise()
