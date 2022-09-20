from unittest.mock import patch


# Facilitate importing Pub/Sub mocks across a wide range of previous versions of `octue`.
try:
    from octue.cloud.emulators._pub_sub import MockSubscriber, MockSubscription, MockTopic
except ModuleNotFoundError:
    try:
        from tests.cloud.pub_sub.mocks import MockSubscriber, MockSubscription, MockTopic
    except ModuleNotFoundError:
        from octue.cloud.emulators.pub_sub import MockSubscriber, MockSubscription, MockTopic


class ServicePatcher:
    def __init__(self, patches=None):
        self.patches = patches or [
            patch("octue.cloud.pub_sub.service.Topic", new=MockTopic),
            patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription),
            patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber),
        ]

    def __enter__(self):
        """Start the patches and return the mocks they produce.

        :return list(unittest.mock.MagicMock):
        """
        return [patch.start() for patch in self.patches]

    def __exit__(self, *args, **kwargs):
        """Stop the patches.

        :return None:
        """
        for p in self.patches:
            p.stop()
