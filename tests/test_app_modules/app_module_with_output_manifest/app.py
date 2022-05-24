from octue.resources import Manifest


def run(analysis, *args, **kwargs):
    """Run a mock analysis.

    :param analysis:
    :return None:
    """
    analysis.output_values = [1, 2, 3, 4]
    analysis.output_manifest = Manifest()
