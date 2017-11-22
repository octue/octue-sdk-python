from octue import octue_analysis, add_to_manifest
import json


@octue_analysis
def create_figure_file(analysis):
    """ Saves json file constructed from some analysis results. The json will be used later to create a figure. File can be saved to an arbitrary location, but by default is saved to the current directory
    """

    # Any plotly compliant dict or list that can be converted to json. You can use the Plotly python sdk to construct figures, by adding it to requirements.txt
    fig = {"data": [{"x": ["giraffes", "orangutans", "monkeys"],
                     "y": [20, 14, 23],
                     "type": "bar"
                     }]}

    # Dump the dict to a plain text json file. Note that for more advanced data (e.g. including numpy arrays etc) you
    # may wish to use the serialiser provided with the plotly library
    name = analysis.output_dir + '/zoo_barchart.json'
    with open(name, 'w') as outfile:
        json.dump(fig, outfile)

    # You can either do this here, or in your main run() function definition (or basically anywhere else you like)...
    # but you need to add the created file (which is part of the analysis results) to the output results manifest. In
    # this case we do it here, which has the advantage of keeping file creation and manifesting together; but has the
    # disadvantage of needing to modify your code to pass the analysis around. If you're unable to alter the API of your
    # code; no problem - just do all your manifest creation separately (e.g. at the end of the run function)
    fig_data = {'name': name,
                'short_caption': 'A shortened caption',
                'caption': 'A longer caption, perhaps including some description of why on earth we would want to see a bar chart of different zoo animals'}
    add_to_manifest('figure', name, fig_data)
