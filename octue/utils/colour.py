import binascii


WHITE = "ffffff"


def round_color(h):
    quot = 255 / 5
    return round(h / quot)


def ansi(rgb):
    if isinstance(rgb, str):
        rgb = binascii.a2b_hex(rgb)
    if isinstance(rgb, bytes):
        rgb = list(map(int, rgb))
    red, green, blue = list(map(round_color, rgb))
    return 16 + 36 * red + 6 * green + blue


def colourise(s, foreground=WHITE, background=None):
    if not background:
        return f"\x1b[38;5;{ansi(foreground)}m{s}\x1b[0m"

    return f"\x1b[38;5;{ansi(foreground)};48;5;{ansi(background)}m{s}\x1b[0m"
