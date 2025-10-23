# Asking services questions

## What is a question?

A question is a set of data (input values and/or an input manifest) sent
to a child for processing/analysis. Questions can be:

- **Synchronous ("ask-and-wait"):** A question whose answer is waited
  for in real time

- **Asynchronous ("fire-and-forget"):** A question whose answer is not
  waited for and is instead retrieved later. There are two types:

  - **Regular:** Responses to these questions are automatically stored
    in an event store where they can be
    [retrieved using the Twined CLI](#retrieving-answers-to-asynchronous-questions)

  - **Push endpoint:** Responses to these questions are pushed to an
    HTTP endpoint for asynchronous handling using Octue's
    [django-twined](https://django-twined.readthedocs.io/en/latest/)
    or custom logic in your own webserver.

Questions are always asked to a _revision_ of a service. You can ask a
service a question if you have its [SRUID](../services/#service-names), project ID, and the necessary permissions.

## Asking a question

```python
from octue.twined.resources import Child

child = Child(
    id="my-organisation/my-service:2.1.7",
    backend={"name": "GCPPubSubBackend", "project_id": "my-project"},
)

answer, question_uuid = child.ask(
    input_values={"height": 32, "width": 3},
    input_manifest=manifest,
)

answer["output_values"]
>>> {"some": "data"}

answer["output_manifest"]["my_dataset"].files
>>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>
```

!!! warning

    If you're using an environment other than the `main` environment, then before asking any questions to your Twined
    services, set the `TWINED_SERVICES_TOPIC_NAME` environment variable to the name of the Twined services Pub/Sub topic
    (this is set when [deploying a service network](../deploying_services/#deploying-services-advanced-developers-guide).
    It will be in the form `<environment-name>.octue.twined.services`

!!! note

    Not including a service revision tag will cause your question to be sent to the default revision of the service
    (usually the latest deployed version). This is determined by making a request to a
    [service registry](https://django-twined.readthedocs.io/en/latest/) if one or more
    [registries are defined](#using-a-service-registry). If none of the service registries contain an entry for this
    service, a specific service revision tag must be used.

You can also set the following options when you call `Child.ask`:

- `children` - If the child has children of its own (i.e. grandchildren of the parent), this optional argument can be
  used to override the child's "default" children. This allows you to specify particular versions of grandchildren to
  use (see [this subsection below](#overriding-a-childs-children).
- `subscribe_to_logs` - if true, the child will forward its logs to you
- `allow_local_files` - if true, local files/datasets are allowed in any input manifest you supply
- `handle_monitor_message` - if provided a function, it will be called on any monitor messages from the child
- `record_events` -- if true, events received from the parent while it processes the question are saved to the
  `Child.received_events` property
- `save_diagnostics` - must be one of {"SAVE_DIAGNOSTICS_OFF", "SAVE_DIAGNOSTICS_ON_CRASH", "SAVE_DIAGNOSTICS_ON"}; if
  turned on, allow the input values and manifest (and its datasets) to be saved by the child either all the time or just
  if the analysis fails
- `question_uuid` - if provided, the question will use this UUID instead of a generated one
- `push_endpoint` - if provided, the result and other events produced during the processing of the question will be
  pushed to this HTTP endpoint (a URL)
- `asynchronous` - if true, don't wait for an answer to the question (the result and other events can be
  [retrieved from the event store later](#retrieving-answers-to-asynchronous-questions)
- `cpus` - the number of CPUs to request for the question; defaults to the number set by the child service
- `memory` - the amount of memory to request for the question e.g. "256Mi" or "1Gi"; defaults to the amount set by the
  child service
- `ephemeral_storage` - the amount of ephemeral storage to request for the question e.g. "256Mi" or "1Gi"; defaults to
  the amount set by the child service
- `timeout` - how long in seconds to wait for an answer (`None` by default - i.e. don't time out)

If the question fails:

- If `raise_errors=False`, the unraised error is returned
- If `raise_errors=False` and `max_retries > 0`, the question is retried up to this number of times
- If `raise_errors=False`, `max_retries > 0`, and `prevent_retries_when` is a list of exception types, the question is
  retried unless the error type is in the list
- If `raise_errors=False`, `log_errors=True`, and the question fails after its final retry, the error is logged

### Exceptions raised by a child

If a child raises an exception while processing your question, the
exception will always be forwarded and re-raised in your local service
or python session. You can handle exceptions in whatever way you like.

### Timeouts

If setting a timeout, bear in mind that the question has to reach the
child, the child has to run its analysis on the inputs sent to it (this
will most likely make up the dominant part of the wait time), and the
answer has to be sent back to the parent. If you're not sure how long a
particular analysis might take, it's best to set the timeout to `None`
initially or ask the owner/maintainer of the child for an estimate.

## Retrieving answers to asynchronous questions

To retrieve results and other events from the processing of a question
later, make sure you have the permissions to access the event store and
run:

```python
from octue.twined.cloud.pub_sub.bigquery import get_events

events = get_events(question_uuid="53353901-0b47-44e7-9da3-a3ed59990a71")
```

**Options**

- `table_id` - If you're not using the standard deployment, you can
  specify a different table here
- `question_uuid` - Retrieve events from this specific question
- `parent_question_uuid` - Retrieve events from questions triggered by
  the same parent question (this doesn't include the parent question's
  events)
- `originator_question_uuid` - Retrieve events for the entire tree of
  questions triggered by an originator question (a question asked
  manually through `Child.ask`; this does include the originator
  question's events)
- `kind` - Only retrieve this kind of event if present (e.g. "result")
- `include_backend_metadata` - If `True`, retrieve information about the
  service backend that produced the event
- `limit` - If set to a positive integer, limit the number of events
  returned to this

!!! note

    Only one of `question_uuid`, `parent_question_uuid`, and `originator_question_uuid` can be provided at one time.

??? example "See an example output here..."

    ``` python
    >>> events
    [
      {
        "event": {
          "kind": "delivery_acknowledgement",
        },
      },
      {
        "event": {
          "kind": "log_record",
          "log_record": {
            "args": null,
            "created": 1709739861.5949728,
            "exc_info": null,
            "exc_text": null,
            "filename": "app.py",
            "funcName": "run",
            "levelname": "INFO",
            "levelno": 20,
            "lineno": 28,
            "module": "app",
            "msecs": 594.9728488922119,
            "msg": "Finished example analysis.",
            "name": "app",
            "pathname": "/workspace/example_service_cloud_run/app.py",
            "process": 2,
            "processName": "MainProcess",
            "relativeCreated": 8560.13798713684,
            "stack_info": null,
            "thread": 68328473233152,
            "threadName": "ThreadPoolExecutor-0_2"
          }
        },
      },
      {
        "event": {
          "kind": "heartbeat",
        },
      },
      {
        "event": {
          "kind": "result",
          "output_manifest": {
            "datasets": {
              "example_dataset": {
                "files": [
                  "gs://octue-sdk-python-test-bucket/example_output_datasets/example_dataset/output.dat"
                ],
                "id": "419bff6b-08c3-4c16-9eb1-5d1709168003",
                "labels": [],
                "name": "divergent-strange-gharial-of-pizza",
                "path": "https://storage.googleapis.com/octue-sdk-python-test-bucket/example_output_datasets/example_dataset/.signed_metadata_files/divergent-strange-gharial-of-pizza",
                "tags": {}
              }
            },
            "id": "a13713ae-f207-41c6-9e29-0a848ced6039",
            "name": null
          },
          "output_values": [1, 2, 3, 4, 5]
        },
      },
    ]
    ```

---

## Asking multiple questions in parallel

You can also ask multiple questions to a service in parallel - just
provide questions as dictionaries of `Child.ask` arguments:

```python
child.ask_multiple(
    {"input_values": {"height": 32, "width": 3}},
    {"input_values": {"height": 12, "width": 10}},
    {"input_values": {"height": 7, "width": 32}},
)
>>> [
        ({"output_values": {"some": "output"}, "output_manifest": None}, '2681ef4e-4ab7-4cf9-8783-aad982d5e324'),
        ({"output_values": {"another": "result"}, "output_manifest": None}, '474923bd-14b6-4f4c-9bfe-8148358f35cd'),
        ({"output_values": {"different": "result"}, "output_manifest": None}, '9a50daae-2328-4728-9ddd-b2252474f118'),
    ]
```

This method uses multithreading, allowing all the questions to be asked
at once instead of one after another.

!!! hint

    The maximum number of threads that can be used to ask questions in
    parallel can be set via the `max_workers` argument. It has no effect on
    the total number of questions that can be asked, just how many can be in
    progress at once.

## Asking a question within a service

If you have
[created your own Twined service](../creating_services) and want to ask children questions, you can do this more
easily than above. Children are accessible from the `analysis` object by
the keys you give them in the
[service configuration](../creating_services/#octueyaml) file. For example, you can ask an `elevation` service a
question like this:

```python
answer, question_uuid = analysis.children["elevation"].ask(
    input_values={"longitude": 0, "latitude": 1}
)
```

if these values are in your service configuration file:

```json
{
  "children": [
    {
      "key": "wind_speed",
      "id": "template-child-services/wind-speed-service:2.1.1",
      "backend": {
        "name": "GCPPubSubBackend",
        "project_id": "my-project"
      }
    },
    {
      "key": "elevation",
      "id": "template-child-services/elevation-service:3.1.9",
      "backend": {
        "name": "GCPPubSubBackend",
        "project_id": "my-project"
      }
    }
  ]
}
```

and your `twine.json` file includes the child keys in its `children`
field:

```json
{
  "children": [
    {
      "key": "wind_speed",
      "purpose": "A service that returns the average wind speed for a given latitude and longitude."
    },
    {
      "key": "elevation",
      "purpose": "A service that returns the elevation for a given latitude and longitude."
    }
  ]
}
```

See the parent service's [service
configuration](https://github.com/octue/octue-sdk-python/blob/main/octue/twined/templates/template-child-services/parent_service/octue.yaml)
and [app.py
file](https://github.com/octue/octue-sdk-python/blob/main/octue/twined/templates/template-child-services/parent_service/app.py)
in the [child-services app
template](https://github.com/octue/octue-sdk-python/tree/main/octue/twined/templates/template-child-services)
to see this in action.

## Overriding a child's children

If the child you're asking a question to has its own children (static
children), you can override these by providing the IDs of the children
you want it to use (dynamic children) to the
`Child.ask` method. Questions that would have gone to the static
children will instead go to the dynamic children. Note that:

- You must provide the children in the same format as they're provided
  in the [service configuration](/creating_services/#octueyaml)
- If you override one static child, you must override others, too
- The dynamic children must have the same keys as the static children
  (so the child knows which service to ask which questions)
- You should ensure the dynamic children you provide are compatible with
  and appropriate for questions from the child service

For example, if the child requires these children in its service
configuration:

```json
[
  {
    "key": "wind_speed",
    "id": "template-child-services/wind-speed-service:2.1.1",
    "backend": {
      "name": "GCPPubSubBackend",
      "project_id": "octue-sdk-python"
    }
  },
  {
    "key": "elevation",
    "id": "template-child-services/elevation-service:3.1.9",
    "backend": {
      "name": "GCPPubSubBackend",
      "project_id": "octue-sdk-python"
    }
  }
]
```

then you can override them like this:

```python
answer, question_uuid = child.ask(
    input_values={"height": 32, "width": 3},
    children=[
        {
            "key": "wind_speed",
            "id": "my/own-service:1.0.0",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_id": "octue-sdk-python"
            },
        },
        {
            "key": "elevation",
            "id": "organisation/another-service:0.1.0",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_id": "octue-sdk-python"
            },
        },
    ],
)
```

### Overriding beyond the first generation

It's an intentional choice to only go one generation deep with
overriding children. If you need to be able to specify a whole tree of
children, grandchildren, and so on, please [upvote this
issue.](https://github.com/octue/octue-sdk-python/issues/528)

## Using a service registry

When asking a question, you can optionally specify one or more [service
registries](https://django-twined.readthedocs.io/en/latest/) to resolve
SRUIDs against. This checks if the service revision exists (good for
catching typos in SRUIDs) and raises an error if it doesn't. Service
registries can also get the default revision of a service if you don't
provide a revision tag. Asking a question if without specifying a
registry will bypass these checks.

### Specifying service registries

You can specify service registries in two ways:

1.  For all questions asked inside a service. In the service
    configuration (`octue.yaml` file):

    > ```yaml
    > services:
    >   - namespace: my-organisation
    >     name: my-app
    >     service_registries:
    >       - name: my-registry
    >         endpoint: blah.com/services
    > ```

2.  For questions to a specific child, inside or outside a service:

    > ```python
    > child = Child(
    >     id="my-organisation/my-service:1.1.0",
    >     backend={"name": "GCPPubSubBackend", "project_id": "my-project"},
    >     service_registries=[
    >         {"name": "my-registry", "endpoint": "blah.com/services"},
    >     ]
    > )
    > ```
