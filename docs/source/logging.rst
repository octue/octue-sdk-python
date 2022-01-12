.. _child_services:

=======
Logging
=======

By default, ``octue`` leaves handling of log messages raised by your app to you. However, if you just need a simple
logging arrangement, you can let ``octue`` format and stream your logs to ``stderr``. If this is for you, simply set
``USE_OCTUE_LOG_HANDLER=1`` in the environment running your app.


Here are some example logs handled by ``octue`` produced when running the child services template app:

.. code-block:: shell

    [2021-07-10 20:03:12,713 | INFO | octue.runner | analysis-102ee7d5-4b94-4f8a-9dcd-36dbd00662ec] Hello! The child services template app is running!
    [2021-07-10 20:03:20,299 | INFO | octue.cloud.pub_sub.service] <Service('thankful-cockle')> asked a question 'ffc37e30-367f-41e8-83d6-38e39f349ce9' to service '71802bcd-e85b-4428-be6a-848c956781f2'.
    [2021-07-10 20:03:20,700 | INFO | octue.cloud.pub_sub.service] <Service('free-doberman')> received a question.
    [2021-07-10 20:03:20,710 | INFO | octue.cloud.pub_sub.service] <Service('free-doberman')> responded to question 'ffc37e30-367f-41e8-83d6-38e39f349ce9'.
    [2021-07-10 20:03:23,812 | INFO | octue.cloud.pub_sub.service] <Service('thankful-cockle')> received an answer to question 'ffc37e30-367f-41e8-83d6-38e39f349ce9'.
    [2021-07-10 20:03:36,929 | INFO | octue.cloud.pub_sub.service] <Service('pragmatic-griffin')> asked a question '437c58d4-4ffe-438b-b57b-2292ece0d2e7' to service '6cc4aadd-bf66-465e-84f3-3ce8b279fa8e'.
    [2021-07-10 20:03:37,845 | INFO | octue.cloud.pub_sub.service] <Service('prophetic-dolphin')> received a question.
    [2021-07-10 20:03:37,849 | INFO | octue.cloud.pub_sub.service] <Service('prophetic-dolphin')> responded to question '437c58d4-4ffe-438b-b57b-2292ece0d2e7'.
    [2021-07-10 20:03:40,401 | INFO | octue.cloud.pub_sub.service] <Service('pragmatic-griffin')> received an answer to question '437c58d4-4ffe-438b-b57b-2292ece0d2e7'.
    [2021-07-10 20:03:44,613 | INFO | octue.runner | analysis-102ee7d5-4b94-4f8a-9dcd-36dbd00662ec] The wind speeds and elevations at [{'longitude': 0, 'latitude': 0}, {'longitude': 1, 'latitude': 1}] are [3296, 1909] and [89, 82].


Adding additional metadata to the log context
--------------––-----------------------------

You can also provide environment variables to add the following metadata to the log context:

- ``INCLUDE_LINE_NUMBER_LOG_METADATA`` - include the line number
- ``INCLUDE_PROCESS_NAME_LOG_METADATA`` - include the process name
- ``INCLUDE_THREAD_NAME_LOG_METADATA`` - include the thread name
