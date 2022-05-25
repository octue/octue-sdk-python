class App:
    """A mock app.

    :param octue.resources.analysis.Analysis analysis:
    :return None:
    """

    def __init__(self, analysis):
        self.analysis = analysis

    def run(self):
        """Set the output values to a string.

        :return None:
        """
        self.analysis.output_values = "App as a class works!"
