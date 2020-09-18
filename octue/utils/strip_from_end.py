def strip_from_end(text, suffix):
    """ Strip a suffix from text, if it appears (otherwise return text unchanged)
    """
    if not text.endswith(suffix):
        return text
    return text[: len(text) - len(suffix)]
