import time
from tests.base import BaseTestCase

from octue.resources.service import Service


class TestService(BaseTestCase):
    def test_serve_with_timeout(self):
        """ Test that a Service can serve for a given time interval and stop at the end of it. """
        service = Service(name="hello")
        start_time = time.perf_counter()
        service.serve(timeout=0)
        duration = time.perf_counter() - start_time
        self.assertTrue(duration < 5)  # Allow for time spent connecting to Google.
