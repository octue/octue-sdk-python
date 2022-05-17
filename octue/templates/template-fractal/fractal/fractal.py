from .mandelbrot import mandelbrot


def fractal(analysis):
    """Compute the heightmap of a fractal and output a data file containing visualisation data.

    :param octue.resources.Analysis analysis:
    :return (dict, dict):
    """
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
        "x": x.tolist(),
        "y": y.tolist(),
        "z": z.tolist(),
        "colorscale": analysis.configuration_values["color_scale"],
        "type": "surface",
    }

    layout = {
        "title": f"Mandelbrot set with {analysis.configuration_values['n_iterations']} iterations",
        "width": analysis.configuration_values["width"],
        "height": analysis.configuration_values["height"],
    }

    return data, layout
