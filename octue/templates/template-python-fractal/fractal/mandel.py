import numpy


def mandel(width, height, x_min, x_max, y_min, y_max, max_iterations, c=None):
    """ Creates a mandelbrot or julia set fractal, given input parameters

    Each iteration, the new value of a pixel is calculated as z = z^2 + c, where for a mandelbrot set (default)
    c is a function of the position in the complex plane (c = x + iy) or a optionally for a julia set, c is a constant

    :parameter width: Integer width of the final fractal image in pixels
    :parameter height: Integer height of the final fractal image in pixels
    :parameter x_min: The minimum value of x for which the fractal will be drawn
    :parameter x_max: The maximum value of x for which the fractal will be drawn
    :parameter y_min: The minimum value of y for which the fractal will be drawn
    :parameter y_max: The maximum value of y for which the fractal will be drawn
    :parameter max_iterations: the iteration limit
    :parameter c: Optional 2-tuple (or other iterable) containing real and complex parts of constant coefficient c.
    Giving this argument will result in creation of a julia set, not the default mandelbrot set
    :return: x, y, z values of pixel locations in the complex plane and a corresponding heightmap z, to build a fancy
    looking 3D fractal
    """

    # Create a linearly spaced 2d grid
    [x, y] = numpy.meshgrid(numpy.linspace(x_min, x_max, width), numpy.linspace(y_min, y_max, height))

    # Preallocate output array
    z = numpy.zeros(height, width)

    # Simple loop to render the fractal set. This is not efficient and would be vectorised in production, but the
    # purpose here is just to provide a simple demo.
    for idx, a in numpy.ndenumerate(x):

        # Get default constant c (Mandelbrot sets use spatial coordinates as constants)
        if c is None:
            b = y(idx)
            c = (a, b)

        x_old = 0.0
        y_old = 0.0
        k = 1
        while (k <= max_iterations) and ((x_old ** 2.0 + y_old ** 2.0) < 4.0):
            x_new = x_old ** 2.0 - y_old ** 2.0 + c[0]
            y_new = 2.0 * x_old * y_old + c[1]
            x_old = x_new
            y_old = y_new
            k += 1

        z[idx] = k

    return x, y, z
