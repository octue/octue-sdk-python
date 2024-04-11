.. _testing_services:

================
Testing services
================
We recommend writing automated tests for your service so anyone who wants to use it can have confidence in its quality
and reliability at a glance. `Here's an example test <https://github.com/octue/example-service-cloud-run/blob/main/tests/test_app.py>`_
for our example service.


Emulating children
==================
If your app has children, you should emulate them in your tests instead of communicating with the real ones. This makes
your tests:

- **Independent** of anything external to your app code  - i.e. independent of the remote child, your internet connection,
  and communication between your app and the child (Google Pub/Sub).
- **Much faster** - the emulation will complete in a few milliseconds as opposed to the time it takes the real child to
  actually run an analysis, which could be minutes, hours, or days. Tests for our `child services template app
  <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-child-services>`_ run
  around **900 times faster** when the children are emulated.

The Child Emulator
------------------
We've written a child emulator that takes a list of events and returns them to the parent for handling in the order
given - without contacting the real child or using Pub/Sub. Any events a real child can produce are supported.
:mod:`Child <octue.resources.child.Child>` instances can be mocked like-for-like by
:mod:`ChildEmulator <octue.cloud.emulators.child.ChildEmulator>` instances without the parent knowing. You can provide
the emulated events in python or via a JSON file.

Message types
-------------
You can emulate any message type that your app (the parent) can handle. The table below shows what these are.

+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Message type          | Number of events supported                                                                     | Example                                                                                                                   |
+=======================+==================================================================================================+===========================================================================================================================+
| ``log_record``        | Any number                                                                                       | {"type": "log_record": "log_record": {"msg": "Starting analysis."}}                                                       |
+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+
| ``monitor_message``   | Any number                                                                                       | {"type": "monitor_message": "data": '{"progress": "35%"}'}                                                                |
+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+
| ``exception``         | One                                                                                              | {"type": "exception", "exception_type": "ValueError", "exception_message": "x cannot be less than 10."}                   |
+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+
| ``result``            | One                                                                                              | {"type": "result", "output_values": {"my": "results"}, "output_manifest": None}                                           |
+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+

**Notes**

- Message formats and contents are validated by :mod:`ChildEmulator <octue.cloud.emulators.child.ChildEmulator>`
- The ``log_record`` key of a ``log_record`` message is any dictionary that the ``logging.makeLogRecord`` function can
  convert into a log record.
- The ``data`` key of a ``monitor_message`` message must be a JSON-serialised string
- Any events after a ``result`` or ``exception`` message won't be passed to the parent because execution of the child
  emulator will have ended.


Instantiating a child emulator in python
----------------------------------------

.. code-block:: python

    events = [
        {
            "type": "log_record",
            "log_record": {"msg": "Starting analysis."},
        },
        {
            "type": "monitor_message",
            "data": '{"progress": "35%"}',
        },
        {
            "type": "log_record",
            "log_record": {"msg": "Finished analysis."},
        },
    	{
            "type": "result",
            "output_values": [1, 2, 3, 4, 5],
            "output_manifest": None,
        },
    ]

    child_emulator = ChildEmulator(
        backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
        events=events
    )

    def handle_monitor_message(message):
        ...

    result = child_emulator.ask(
        input_values={"hello": "world"},
        handle_monitor_message=handle_monitor_message,
    )
    >>> {"output_values": [1, 2, 3, 4, 5], "output_manifest": None}


Instantiating a child emulator from a JSON file
-----------------------------------------------
You can provide a JSON file with either just events in or with events and some or all of the
:mod:`ChildEmulator <octue.cloud.emulators.child.ChildEmulator>` constructor parameters. Here's an example JSON file
with just the events:

.. code-block:: json

    {
        "events": [
            {
                "type": "log_record",
                "log_record": {"msg": "Starting analysis."}
            },
            {
                "type": "log_record",
                "log_record": {"msg": "Finished analysis."}
            },
            {
                "type": "monitor_message",
                "data": "{\"progress\": \"35%\"}"
            },
            {
                "type": "result",
                "output_values": [1, 2, 3, 4, 5],
                "output_manifest": null
            }
        ]
    }

You can then instantiate a child emulator from this in python:

.. code-block:: python

    child_emulator = ChildEmulator.from_file("path/to/emulated_child.json")

    def handle_monitor_message(message):
        ...

    result = child_emulator.ask(
        input_values={"hello": "world"},
        handle_monitor_message=handle_monitor_message,
    )
    >>> {"output_values": [1, 2, 3, 4, 5], "output_manifest": None}


Using the child emulator
------------------------
To emulate your children in tests, patch the :mod:`Child <octue.resources.child.Child>` class with the
:mod:`ChildEmulator <octue.cloud.emulators.child.ChildEmulator>` class.

.. code-block:: python

    from unittest.mock import patch

    from octue import Runner
    from octue.cloud.emulators import ChildEmulator


    app_directory_path = "path/to/directory_containing_app"

    # You can explicitly specify your children here as shown or
    # read the same information in from your app configuration file.
    children = [
        {
            "key": "my_child",
            "id": "octue/my-child-service:2.1.0",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "my-project"
            }
        },
    ]

    runner = Runner(
        app_src=app_directory_path,
        twine=os.path.join(app_directory_path, "twine.json"),
        children=children,
        service_id="your-org/your-service:2.1.0",
    )

    emulated_children = [
        ChildEmulator(
            id="octue/my-child-service:2.1.0",
            internal_service_name="you/your-service:2.1.0",
            events=[
                {
                    "type": "result",
                    "output_values": [300],
                    "output_manifest": None,
                },
            ]
        )
    ]

    with patch("octue.runner.Child", side_effect=emulated_children):
        analysis = runner.run(input_values={"some": "input"})


**Notes**

- If your app uses more than one child, provide more child emulators in the ``emulated_children`` list in the order
  they're asked questions in your app.
- If a given child is asked more than one question, provide a child emulator for each question asked in the same order
  the questions are asked.


Creating a test fixture
=======================
Since the child is *emulated*, it doesn't actually do any calculation - if you change the inputs, the outputs won't
change correspondingly (or at all). So, it's up to you to define a set of realistic inputs and corresponding outputs
(the list of emulated events) to test your service. These are called **test fixtures**.

.. note::
  Unlike a real child, the inputs given to the emulator and the outputs returned aren't validated against the schema in
  the child's twine - this is because the twine is only available to the real child. This is ok - you're testing your
  service, not the child.

You can create test fixtures manually or by using the ``Child.received_events`` property after questioning a real
child.

.. code-block:: python

    import json
    from octue.resources import Child


    child = Child(
        id="octue/my-child:2.1.0",
        backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
    )

    result = child.ask(input_values=[1, 2, 3, 4])

    child.received_events
    >>> [
            {
                'type': 'delivery_acknowledgement',
                'delivery_time': '2022-08-16 11:49:57.244263',
            },
            {
                'type': 'log_record',
                'log_record': {
                    'msg': 'Finished analysis.',
                    'args': None,
                    'levelname': 'INFO',
                    ...
                },
            },
            {
                'type': 'result',
                'output_values': {"some": "results"},
                'output_manifest': None,
            }
        ]

You can then feed these into a child emulator to emulate one possible response of the child:

.. code-block:: python

    from octue.cloud.emulators import ChildEmulator


    child_emulator = ChildEmulator(events=child.received_events)

    child_emulator.ask(input_values=[1, 2, 3, 4])
    >>> {"some": "results"}

You can also create test fixtures from :ref:`downloaded service crash diagnostics <test_fixtures_from_crash_diagnostics>`.
