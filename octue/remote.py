from .exceptions import NotImplementedYet


def remote():
    """ Registers and cryptographically signs a local analysis

    The registration process cryptographically signs inputs and outputs, generating hashes and UUIDs for later
    registration on the Octue platform.

    This allows analyses to run remotely (e.g. on an instrument that is infrequently accessed), and results immediately
    sent to Octue whilst upload of large input data is delayed.

    A classic example of this is in ship-to-shore communication where bandwidth is expensive. Sensors gather
    high-frequency data and pre-process it. Low frequency results (e.g. weather metrics) might be sent via satellite,
    but high temporal resolution data stored on disk on-board.

    """

    raise NotImplementedYet('Only enterprise users in closed beta can use remote processing capabilities. Please contact support@octue.com for more information')
