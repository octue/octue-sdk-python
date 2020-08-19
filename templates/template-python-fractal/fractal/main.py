from octue import analysis

from .mandel import mandel


def mandelbrot():
    """ A function that does some analysis, and saves the result to a data file.
    This can be a legacy module if necessary - note the complete lack of octue dependencies in this case
    (everything is passed back to app.py)
    """

    # Call the 'mandel' function to compute the fractal. Here, we just treat 'mandel' as a legacy function, passing in
    # what we need from the analysis object and getting results back
    x, y, z = mandel(
        analysis.config["width"],
        analysis.config["height"],
        analysis.config["x_min"],
        analysis.config["x_max"],
        analysis.config["y_min"],
        analysis.config["y_max"],
        analysis.config["max_iterations"],
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
        "colorscale": analysis.config["color_scale"],
        "type": "surface",
    }
    layout = {
        "title": f"Mandelbrot set with {analysis.config['max_iterations']} iterations",
        "width": analysis.config.width,
        "height": analysis.config.height,
    }

    # We'll add some tags, which will help to improve searchability and allow
    # other apps, reports, users and analyses to automatically find figures and
    # use them.
    #
    # Get descriptive with tags... they are whitespace-delimited and colons can be
    # used to provide subtags. Tags are case insensitive, and accept a-z, 0-9,
    # hyphens and underscores (which can be used literally in search and are also
    # used to separate words in natural language search). Other special characters
    # will be stripped.
    tags = "contents:fractal:mandelbrot type:figure:surface"

    # Figures are just a slightly special kind of results file. This causes the figure to be saved as an output
    # add_figure(data, layout, tags)
    return data, layout, tags
