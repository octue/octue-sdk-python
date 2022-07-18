import numpy


def mandelbrot(width, height, x_range, y_range, n_iterations, c=None):
    """Compute the heightmap for a mandelbrot or julia set fractal.

    Each iteration, the new value of a pixel is calculated as z = z^2 + c, where for a mandelbrot set (default)
    c is a function of the position in the complex plane (c = x + iy) or a optionally for a julia set, c is a constant

    :param int width: Integer width of the final fractal image in pixels
    :param int height: Integer height of the final fractal image in pixels
    :param list(float)|numpy.ndarray x_range: The range of x [min, max] for which the fractal will be drawn
    :param list(float)|numpy.ndarray y_range: The range of y [min, max] for which the fractal will be drawn
    :param int n_iterations: the number of iterations limit used to compute the fractal
    :param None|(float, float) c: Optional 2-tuple (or other iterable) containing real and complex parts of constant coefficient c. Giving this argument will result in creation of a julia set, not the default mandelbrot set
    :return (numpy.ndarray, numpy.ndarray, numpy.ndarray): x, y, z values of pixel locations in the x, y complex plane and a corresponding heightmap z, with which you can lot a fancy looking 3d fractal
    """
    # Create a linearly spaced 2d grid
    [x, y] = numpy.meshgrid(
        numpy.linspace(x_range[0], x_range[1], width), numpy.linspace(y_range[0], y_range[1], height)
    )

    # Preallocate output array
    z = numpy.zeros((height, width))

    # Simple loop to render the fractal set. This is not efficient python and would be vectorised in production, but the
    # purpose here is just to provide a simple demo.
    for idx, a in numpy.ndenumerate(x):

        # Get default constant c (Mandelbrot sets use spatial coordinates as constants)
        if c is None:
            b = y[idx]
            c = (a, b)

        x_old = 0.0
        y_old = 0.0
        k = 1
        while (k <= n_iterations) and ((x_old**2.0 + y_old**2.0) < 4.0):
            x_new = x_old**2.0 - y_old**2.0 + c[0]
            y_new = 2.0 * x_old * y_old + c[1]
            x_old = x_new
            y_old = y_new
            k += 1

        z[idx] = k

    return x, y, z
