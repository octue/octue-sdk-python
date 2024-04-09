class MultiPatcher:
    """A patch context manager that applies multiple patches at once. All the given patches are started on enter and
    stopped on exit.

    :param list(unittest.mock._patch) patches:
    :return None:
    """

    def __init__(self, patches=None):
        self.patches = patches or []

    def __enter__(self):
        """Start the patches and return the mocks they produce.

        :return list(unittest.mock.MagicMock):
        """
        return self.start()

    def __exit__(self, *args, **kwargs):
        """Stop the patches.

        :return None:
        """
        self.stop()

    def start(self):
        """Start the patches and return the mocks they produce.

        :return list(unittest.mock.MagicMock):
        """
        return [patch.start() for patch in self.patches]

    def stop(self):
        """Stop the patches.

        :return None:
        """
        for patch in self.patches:
            patch.stop()
