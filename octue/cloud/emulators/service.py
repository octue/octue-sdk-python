from unittest.mock import patch

from octue.cloud.emulators._pub_sub import MockSubscriber, MockSubscription, MockTopic
from octue.utils.patches import MultiPatcher


class ServicePatcher(MultiPatcher):
    """A multi-patcher that provides the patches needed to run mock services.

    :return None:
    """

    def __init__(self):
        super().__init__(
            patches=[
                patch("octue.cloud.pub_sub.service.Topic", new=MockTopic),
                patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription),
                patch("octue.cloud.pub_sub.events.SubscriberClient", new=MockSubscriber),
                patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber),
            ]
        )
