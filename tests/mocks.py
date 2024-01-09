import io
import os


class MockOpen:
    """A mock for patching `builtins.open` that returns different text streams depending on the path given to it. To
    set these, override the class variable `path_to_contents_mapping` with a dictionary mapping the paths to the
    desired output.

    :param str path:
    :param kwargs: any kwargs that the builtin `open` supports
    :return None:
    """

    path_to_contents_mapping = {}

    def __init__(self, path, **kwargs):
        self.__dict__ = {**kwargs}
        self.path = path

    def __enter__(self):
        try:
            return io.StringIO(self.path_to_contents_mapping[self.path])

        # Allow absolute paths that end in the (in this case, relative) paths given in `path_to_contents_mapping`.
        except KeyError:
            path = os.path.split(self.path)[-1]
            return io.StringIO(self.path_to_contents_mapping[path])

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
