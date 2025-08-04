from octue.resources import Manifest


def run(analysis):
    """Run a mock analysis.

    :param octue.resources.analysis.Analysis analysis:
    :return None:
    """
    analysis.output_values = {"width": 3}
    analysis.output_manifest = Manifest()
