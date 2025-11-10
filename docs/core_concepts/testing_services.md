We recommend writing automated tests for your service so anyone who
wants to use it can have confidence in its quality and reliability at a
glance. [Here's an example
test](https://github.com/octue/example-service-kueue/blob/main/tests/test_app.py)
for our example service.

## Emulating children

If your app has children, you should emulate them in your tests instead
of communicating with the real ones. This makes your tests:

- **Independent** of anything external to your app code - i.e.
  independent of the remote child, your internet connection, and
  communication between your app and the child (Google Pub/Sub).
- **Much faster** - the emulation will complete in a few milliseconds as
  opposed to the time it takes the real child to actually run an
  analysis, which could be minutes, hours, or days. Tests for our [child
  services template
  app](https://github.com/octue/octue-sdk-python/tree/main/octue/twined/templates/template-child-services)
  run around **900 times faster** when the children are emulated.

### The Child Emulator

We've written a child emulator that takes a list of events and returns
them to the parent for handling in the order given - without contacting
the real child or using Pub/Sub. Any events a real child can produce are
supported. `Child` instances can be mocked like-for-like by
`ChildEmulator` instances without the parent knowing.

### Event kinds

You can emulate any event that your app (the parent) can handle. The
table below shows what these are.

| Event kind                 | Number of events supported | Example                                                                                                                                                                                           |
| -------------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `delivery_acknowledgement` | One                        | `{"event": {"kind": "delivery_acknowledgement"}, "attributes": {"question_uuid": "79192e90-9022-4797-b6c7-82dc097dacdb", ...}}`                                                                   |
| `heartbeat`                | Any number                 | `{"event": {"kind": "heartbeat"}, "attributes": {"question_uuid": "79192e90-9022-4797-b6c7-82dc097dacdb", ...}`                                                                                   |
| `log_record`               | Any number                 | `{"event": {"kind": "log_record": "log_record": {"msg": "Starting analysis."}}, "attributes": {"question_uuid": "79192e90-9022-4797-b6c7-82dc097dacdb", ...}`                                     |
| `monitor_message`          | Any number                 | `{"event": {"kind": "monitor_message": "data": '{"progress": "35%"}'}, "attributes": {"question_uuid": "79192e90-9022-4797-b6c7-82dc097dacdb", ...}`                                              |
| `exception`                | One                        | `{"event": {"kind": "exception", "exception_type": "ValueError", "exception_message": "x cannot be less than 10."}, "attributes": {"question_uuid": "79192e90-9022-4797-b6c7-82dc097dacdb", ...}` |
| `result`                   | One                        | `{"event": {"kind": "result", "output_values": {"my": "results"}, "output_manifest": None}, "attributes": {"question_uuid": "79192e90-9022-4797-b6c7-82dc097dacdb", ...}`                         |

**Notes**

- Event formats and contents must conform with the [service
  communication
  schema](https://strands.octue.com/octue/service-communication).
- Every event must be accompanied with the required event attributes
- The `log_record` key of a `log_record` event is any dictionary that
  the `logging.makeLogRecord` function can convert into a log record.
- The `data` key of a `monitor_message` event must be a JSON-serialised
  string
- Any events after a `result` or `exception` event won't be passed to
  the parent because execution of the child emulator will have ended.

### Instantiating a child emulator

```python
events = [
    {
        {
            "event": {
                "kind": "log_record",
                "log_record": {"msg": "Starting analysis."},
                ... # Left out for brevity.
            },
            "attributes": {
                "datetime": "2024-04-11T10:46:48.236064",
                "uuid": "a9de11b1-e88f-43fa-b3a4-40a590c3443f",
                "retry_count": 0,
                "question_uuid": "d45c7e99-d610-413b-8130-dd6eef46dda6",
                "parent_question_uuid": "5776ad74-52a6-46f7-a526-90421d91b8b2",
                "originator_question_uuid": "86dc55b2-4282-42bd-92d0-bd4991ae7356",
                "parent": "octue/test-service:1.0.0",
                "originator": "octue/test-service:1.0.0",
                "sender": "octue/test-service:1.0.0",
                "sender_type": "CHILD",
                "sender_sdk_version": "0.51.0",
                "recipient": "octue/another-service:3.2.1"
            },
        },
    },
    {
         "event": {
             "kind": "monitor_message",
             "data": '{"progress": "35%"}',
         },
         "attributes": {
             ...  # Left out for brevity.
         },
    },
    {
        "event": {
            "kind": "log_record",
            "log_record": {"msg": "Finished analysis."},
            ... # Left out for brevity.
        },
        "attributes": {
            ...  # Left out for brevity.
        },
    },
    {
        "event": {
            "kind": "result",
            "output_values": [1, 2, 3, 4, 5],
        },
        "attributes": {
            ...  # Left out for brevity.
        },
    },
]

child_emulator = ChildEmulator(events)

def handle_monitor_message(message):
    ...

result, question_uuid = child_emulator.ask(
    input_values={"hello": "world"},
    handle_monitor_message=handle_monitor_message,
)
>>> {"output_values": [1, 2, 3, 4, 5], "output_manifest": None}
```

### Using the child emulator

To emulate your children in tests, patch the `Child` class with the `ChildEmulator` class.

```python
from unittest.mock import patch

from octue.twined.runner import Runner
from octue.twined.cloud.emulators import ChildEmulator


app_directory_path = "path/to/directory_containing_app"

# You can explicitly specify your children here as shown or
# read the same information in from your service configuration file.
children = [
    {
        "key": "my_child",
        "id": "octue/my-child-service:2.1.0",
        "backend": {
            "name": "GCPPubSubBackend",
            "project_id": "my-project"
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
        events=[
            {
                "event": {
                    "kind": "result",
                    "output_values": [300],
                },
                "attributes": {
                    "datetime": "2024-04-11T10:46:48.236064",
                    "uuid": "a9de11b1-e88f-43fa-b3a4-40a590c3443f",
                    "retry_count": 0,
                    "question_uuid": "d45c7e99-d610-413b-8130-dd6eef46dda6",
                    "parent_question_uuid": "5776ad74-52a6-46f7-a526-90421d91b8b2",
                    "originator_question_uuid": "86dc55b2-4282-42bd-92d0-bd4991ae7356",
                    "parent": "you/your-service:2.1.0",
                    "originator": "you/your-service:2.1.0",
                    "sender": "octue/my-child-service:2.1.0",
                    "sender_type": "CHILD",
                    "sender_sdk_version": "0.56.0",
                    "recipient": "you/your-service:2.1.0"
                },
            },
        ],
    )
]

with patch("octue.runner.Child", side_effect=emulated_children):
    analysis = runner.run(input_values={"some": "input"})

analysis.output_values
>>> [300]

analysis.output_manifest
>>> None
```

**Notes**

- If your app uses more than one child, provide more child emulators in
  the `emulated_children` list in the order they're asked questions in
  your app.
- If a given child is asked more than one question, provide a child
  emulator for each question asked in the same order the questions are
  asked.

## Creating a test fixture

Since the child is _emulated_, it doesn't actually do any calculation -
if you change the inputs, the outputs won't change correspondingly (or
at all). So, it's up to you to define a set of realistic inputs and
corresponding outputs (the list of emulated events) to test your
service. These are called **test fixtures**.

!!! note

    Unlike a real child, the **inputs** given to the emulator aren't
    validated against the schema in the child's twine -this is because the
    twine is only available to the real child. This is ok - you're testing
    your service, not the child your service contacts. The events given to
    the emulator are still validated against the service communication
    schema, though.

You can create test fixtures manually or by using the
`Child.received_events` property after questioning a real child.

```python
import json
from octue.twined.resources import Child


child = Child(
    id="octue/my-child:2.1.0",
    backend={"name": "GCPPubSubBackend", "project_id": "my-project"},
)

result, question_uuid = child.ask(input_values=[1, 2, 3, 4])

child.received_events
>>> [
        {
            "event": {
                'kind': 'delivery_acknowledgement',
            },
            "attributes": {
                ... # Left out for brevity.
            },
        },
        {
            "event": {
                'kind': 'log_record',
                'log_record': {
                    'msg': 'Finished analysis.',
                    'args': None,
                    'levelname': 'INFO',
                    ... # Left out for brevity.
                },
            },
            "attributes": {
                ... # Left out for brevity.
            },
        },
        {
            "event": {
                'kind': 'result',
                'output_values': {"some": "results"},
            },
            "attributes": {
                ... # Left out for brevity.
            },
        },
    ]
```

You can then feed these into a child emulator to emulate one possible
response of the child:

```python
from octue.twined.cloud.emulators import ChildEmulator


child_emulator = ChildEmulator(events=child.received_events)
result, question_uuid = child_emulator.ask(input_values=[1, 2, 3, 4])

result
>>> {"some": "results"}
```

You can also create test fixtures from
[downloaded service crash diagnostics](troubleshooting_services.md/#creating-test-fixtures-from-diagnostics).
