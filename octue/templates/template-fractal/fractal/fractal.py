import json
import os

import coolname

from octue.resources import Datafile
from octue.utils.encoders import OctueJSONEncoder

from .mandelbrot import mandelbrot


def fractal(analysis):
    """Compute the heightmap of a fractal and output a data file containing visualisation data"""
    # Call the 'mandel' function to compute the fractal. Here, we just treat 'mandel' as a legacy function, passing in
    # what we need from the analysis object and getting results back
    x, y, z = mandelbrot(
        analysis.configuration_values["width"],
        analysis.configuration_values["height"],
        analysis.configuration_values["x_range"],
        analysis.configuration_values["y_range"],
        analysis.configuration_values["n_iterations"],
    )

    # The best thing to do with a fractal is to plot it. Let's do that.
    #
    # Figures are based on the Plotly figure spec, and you can use any of the plotly python tools here to create, show
    # and develop your figures while running locally.
    #
    # The beauty of the plotly system is that it's entirely json based, which means we can deliver figures and
    # dashboards as outputs (or monitors updated in real time) from digital twins and data services.
    #
    # Often, it's quicker to create the data and layout yourself than to use plotly's graph_objects library, so we do
    # that here:
    data = {
        "x": x,
        "y": y,
        "z": z,
        "colorscale": analysis.configuration_values["color_scale"],
        "type": "surface",
    }

    layout = {
        "title": f"Mandelbrot set with {analysis.configuration_values['n_iterations']} iterations",
        "width": analysis.configuration_values["width"],
        "height": analysis.configuration_values["height"],
    }

    # We'll add some labels and tags, which will help to improve searchability and allow other apps, reports, users and
    # analyses to automatically find figures and use them.
    #
    # Labels are case-insensitive, and accept a-z, 0-9, and hyphens which can be used literally in search and are also
    # used to separate words in natural language search. Tags are key value pairs where the values can be anything but
    # the keys only accept a-z, 0-9, and underscores.
    labels = {"complex-figure"}
    tags = {"figure_contents": "fractal:mandelbrot"}

    # Get the output dataset which will be used for storing the figure file(s)
    output_dataset = analysis.output_manifest.get_dataset("fractal_figure_files")

    # Create a Datafile to hold the figure. We could put it in the current directory, but it makes sense to put it in a
    # unique folder for this output dataset - doing so avoids any race conditions arising (if other instances of this
    # application are running at the same time) and avoids data loss.

    datafile = Datafile(
        # File name including extension (and can include subfolders within the dataset).
        path=os.path.join(coolname.generate_slug(2), "my_mandelbrot_file.json"),
        tags=tags,
        labels=labels,
    )

    # Write the data to the datafile.
    with datafile.open("w") as f:
        # The special encoder just makes it easy to handle numpy arrays.
        json.dump({"data": data, "layout": layout}, f, cls=OctueJSONEncoder)

    # And finally we add it to the output dataset.
    output_dataset.add(datafile)
