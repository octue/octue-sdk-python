.. _testing_services:

================
Testing services
================
We recommend writing automated tests for your service so anyone who wants to use it can have a high confidence in its
quality and reliability at a glance. `Here's an example test <https://github.com/octue/example-service-cloud-run/blob/main/tests/test_app.py>`_
for our example service.

Using the Child Emulator
========================
If your app has children, you should emulate them during testing. This makes your tests:
- Independent of anything external to your app code  - i.e. independent of the remote child, your internet connection, and communication between your app and the child (Google Pub/Sub)
- Much faster - the emulation will complete in a few milliseconds as opposed to the time it takes the real child to actually run an analysis, which could be minutes, hours, or days

The Child Emulator takes a list of messages and returns them to the parent for handling in the order given without contacting the real child or using Pub/Sub. Any messages a real child
could produce are supported. :mod:`Child <octue.resources.child.Child>` instances can be replaced or mocked like-for-like by :mod:`ChildEmulator <octue.cloud.emulators.child.ChildEmulator>`
instances without the parent knowing. You can provide the emulated messages in python or via a JSON file.

Message types
-------------
You can emulate any message type that parent services can handle. The table below shows what these are.

+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Message type          | Number of messages supported                                                                     | Example                                                                                                                   |
+=======================+==================================================================================================+===========================================================================================================================+
| ``log_record``        | Any number                                                                                       | {"type": "log_record": "content": {"msg": "Starting analysis."}}                                                          |
+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+
| ``monitor_message``   | Any number                                                                                       | {"type": "monitor_message": "content": "A monitor message."}                                                              |
+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+
| ``exception``         | One (technically, any number, but only the first will be raised)                                 | {"type": "exception", "content": {"exception_type": "ValueError", "exception_message": "x cannot be less than 10."}}      |
+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+
| ``result``            | One (technically, any number, but only the first will be returned)                               | {"type": "result", "content": {"output_values": {"my": "results"}, "output_manifest", None}}                              |
+-----------------------+--------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------+

Any messages after a ``result`` or ``exception`` message won't be passed to the parent (your app) because execution of
the child emulator will have ended.

Input/output validation
-----------------------
Unlike a real child, the inputs given to the emulator and the outputs returned aren't validated against the schema in
the child's twine as this is only available to the real child. Hence, the input values and manifest do not affect the
messages returned by the emulator, and the output values and manifest are exactly what you specify in the messages you
give to the emulator.

Instantiating a Child Emulator in python
----------------------------------------

.. code-block:: python

    messages = [
        {
            "type": "log_record",
            "content": {"msg": "Starting analysis."},
        },
        {
            "type": "monitor_message",
            "content": {"progress": "35%"},
        },
        {
            "type": "log_record",
            "content": {"msg": "Finishing analysis."},
        },
    	{
            "type": "result",
            "content": {"output_values": [1, 2, 3, 4, 5]},
        },
    ]

    child_emulator = ChildEmulator(
        id="emulated-child",
        backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
        messages=messages
    )

    def handle_monitor_message(message):
        ...

    result = child_emulator.ask(
        input_values={"hello": "world"},
        handle_monitor_message=handle_monitor_message,
    )
    >>> {"output_values": [1, 2, 3, 4, 5], "output_manifest": None}


Instantiating a Child Emulator from a JSON file
-----------------------------------------------
You can provide a JSON file with just messages in or with all the constructor parameters of
:mod:`ChildEmulator <octue.cloud.emulators.child.ChildEmulator>`. Here's an example JSON file with just the messages:

.. code-block:: json

    {
        "messages": [
            {
                "type": "log_record",
                "content": {"msg": "Starting analysis."}
            },
            {
                "type": "log_record",
                "content": {"msg": "Finishing analysis."}
            },
            {
                "type": "monitor_message",
                "content": {"progress": "35%"}
            },
            {
                "type": "result",
                "content": {"output_values": [1, 2, 3, 4, 5], "output_manifest": null}
            }
        ]
    }

You can instantiate a child emulator from this in python:

.. code-block:: python

    child_emulator = ChildEmulator.from_file("path/to/emulated_child.json")

    def handle_monitor_message(message):
        ...

    result = child_emulator.ask(
        input_values={"hello": "world"},
        handle_monitor_message=handle_monitor_message,
    )
    >>> {"output_values": [1, 2, 3, 4, 5], "output_manifest": None}


Providing emulated children to a test
-------------------------------------
