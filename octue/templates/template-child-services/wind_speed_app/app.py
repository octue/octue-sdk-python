def run(analysis):

    analysis.output_values = {"wind_speeds": []}

    for location in analysis.input_values:
        print(location)
        analysis.output_values["wind_speeds"].append(location)
