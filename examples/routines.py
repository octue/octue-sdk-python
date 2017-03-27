import numpy as np


def a_legacy_analysis_function(output_file):
    """ A function that does some analysis, and saves the result to a data file.
    Can be a legacy module - note the complete lack of oasys dependencies.
    """

    a = np.array([20,30,40,50])
    b = np.array([0, 1, 2, 3])
    c = a-b

    np.save(output_file, c)

    # Results files need to be added to the manifest. We can do that here, but in this case we've chosen to do it in the
    # app.py file itself, leaving this module totally free of oasys library dependencies. This is the way you'd work if
    # you had legacy (or unmodifiable) analysis code - or code that you want to be reusable (e.g. as a library) outside
    # the oasys framework. In these cases, you don't want to be digging around adding output manifesting.