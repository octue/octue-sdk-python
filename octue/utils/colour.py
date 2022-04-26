# The functions in this module were based on https://github.com/brutasse/rgb2ansi.

import binascii


WHITE = "ffffff"


def colourise(string, text_colour=WHITE, background_colour=None):
    """Colour in a string with the given text colour and, optionally, background colour.

    :param str string: the string to colourise
    :param str text_colour: the hex representation of the text colour
    :param str|None background_colour: the hex representation of the background colour
    :return str: the colourised string
    """
    text_colour = _convert_colour_to_ansi(text_colour)

    if not background_colour:
        return f"\x1b[38;5;{text_colour}m{string}\x1b[0m"

    background_colour = _convert_colour_to_ansi(background_colour)
    return f"\x1b[38;5;{text_colour};48;5;{background_colour}m{string}\x1b[0m"


def _convert_colour_to_ansi(colour):
    if isinstance(colour, str):
        colour = binascii.a2b_hex(colour)

    if isinstance(colour, bytes):
        colour = list(map(int, colour))

    red, green, blue = list(map(_round_colour, colour))
    return 16 + 36 * red + 6 * green + blue


def _round_colour(colour):
    quot = 255 / 5
    return round(colour / quot)
