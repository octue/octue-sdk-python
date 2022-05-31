from octue.resources import Manifest


def run(analysis):
    """Run a mock analysis.

    :param octue.resources.analysis.Analysis analysis:
    :return None:
    """
    analysis.output_values = [1, 2, 3, 4]
    analysis.output_manifest = Manifest()
