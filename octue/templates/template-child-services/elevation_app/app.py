def run(analysis):

    analysis.output_values = {"elevations": []}

    for location in analysis.input_values:
        print(location)
        analysis.output_values["elevations"].append(location)
