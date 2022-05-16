.. _child_services:

==============
Child services
==============

When a Twine file is written, there is the option to include children (child services or child digital twins) so
the main/parent service can communicate with them to ask "questions". A question is a set of input values and/or an
input manifest in the form the child's Twine specifies. When a question is asked, the parent can expect an answer,
which is a set of output values and/or an output manifest produced by the child's analysis of the inputs (again in the
form specified by the child's Twine).

There can be:

- Any number of children
- Any number of questions asked to each child

Further, a child can have its own children that it asks its own questions to as part of analyses it runs. There is no
limit to this as long as the graph of services is a directed acyclic graph (DAG) - i.e. as long as there are no loops
and no children ask their parents any questions.

To help you debug and keep track of a child's progress in answering your question, its logs can be streamed back to the
parent and displayed just like local log messages. They are distinguished from local logs by ``[REMOTE]`` appearing at
the start of their messages. Simply set ``subscribe_to_logs`` to ``True`` (this is the default behaviour; see below).
Any exception raised during the child's analysis will always be forwarded to the parent and raised there with the full
traceback.

A ``timeout`` (measured in seconds) can be set for how long you are willing to wait for an answer, but bear in mind
that the question has to reach the child, the child has to run its own analysis on the inputs sent to it (this most
likely corresponds to the dominant part of the wait time), and the answer has to be sent back to the parent. If you are
not sure how long a particular analysis might take, it's best to set the timeout to ``None`` initially or ask the
owner/maintainer of the child for an estimate.

-------------------------
Example usage in your app
-------------------------

Assuming you have specified which children you would like to use in your ``twine.json`` file (see below for an example),
you can ask children questions in your ``app.py`` file (or packages/modules imported into it) as follows:

.. code-block:: python

    answer_1 = analysis.children["wind_speed"].ask(
        input_values=analysis.input_values,
        input_manifest=analysis.input_manifest,
        subscribe_to_logs=True,  # This means logs from the child's analysis will stream to your machine and appear like other logs.
        timeout=None
    )

    answer_2 = analysis.children["elevation"].ask(input_values=analysis.input_values, timeout=None)

    >>> answer_1
    {
        'output_values': <output values in form specified by child twine.json>,
        'output_manifest': <output manifest in form specified by child twine.json>
    }


--------
Backends
--------

The backend specifies which method of communication the child uses (e.g. Google Cloud Pub/Sub), as well as providing
any parameters required to access it. Each child must have its backend specified explicitly, even if all children use
the same one. This is to support the use case where each child uses a different backend. Note that backends/credentials
are only needed for the children that your app explicitly contacts - it is the responsibility of all children to be
able to contact any children they need to run their analyses (i.e. children can be treated as black boxes).

To make use of a certain child in ``app.py``, its backend configuration must be specified in ``children.json``. The only
backend currently supported is ``GCPPubSubBackend``, which uses Google Cloud Platform's publisher/subscriber service.


--------------------------
Example children.json file
--------------------------

To access children in ``app.py``, they must be specified in ``children.json``, which is by default looked for in
``<data_dir>/configuration/children.json``:

.. code-block:: javascript

    [
        {
            "key": "wind_speed",
            "id": "7b9d07fa-6bcd-4ec3-a331-69f737a15751",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "<google_cloud_project_name>"
            }
        },
        {
            "key": "elevation",
            "id": "8dgd07fa-6bcd-4ec3-a331-69f737a15332",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "<google_cloud_project_name>"
            }
        }
    ]


-------------------------------------------
Example children field in a twine.json file
-------------------------------------------

The children field must also be present in the ``twine.json`` file:

.. code-block:: javascript

    {
        ...
        "children": [
            {
                "key": "wind_speed",
                "purpose": "A service that returns the average wind speed for a given latitude and longitude.",
                "notes": "Some notes.",
                "filters": "labels:wind_speed"
            },
            {
                "key": "elevation",
                "purpose": "A service that returns the elevation for a given latitude and longitude.",
                "notes": "Some notes.",
                "filters": "labels:elevation"
            }
        ],
        ...
    }


------------------------------------
Starting a child/service as a server
------------------------------------

For a parent to ask a child questions, the child must already be running as a server. The person/organisation
responsible for the child must start it as a server if it is to be able to answer questions.

To start a service as a server, the command line interface (CLI) can be used:

.. code-block:: bash

    octue start \
        --app-dir=<path/to/app_directory> \
        --twine=<path/to/twine.json> \
        --config-dir=<path/to/configuration> \
        --service-id=<UUID of service>

You can choose a random UUID for the service ID, but it must be unique across all services. It must also stay the same
once it has been created so that Scientists and other services can know which service is which and communicate with the
correct ones. We recommend registering your service with Octue if you want others to be able to use it easily (and, if
allowed, look it up), and also so that its ID is reserved permanently.

**Note:** We will be automating this process soon. In the meantime, please contact us to register service IDs.


--------------------------------------------------------------------------
See services communicate in real time: running the child services template
--------------------------------------------------------------------------

1. Contact Octue to request a Google Cloud Platform service account credentials file.

2. Save this file locally and create a ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable whose value is the absolute path to a file containing GCP service account credentials. This variable must be available to all three terminal windows used to run the template - see below for one method of doing this. **IMPORTANT**: Do not commit this or any other credentials or credentials file to git, GitHub, or any other version control software or website - doing so opens you, your systems and equipment, and our systems and equipment up to hackers and cyber attack.

3. From the repository root, start the elevation service as a server in a terminal window:

.. code-block:: bash

    GOOGLE_APPLICATION_CREDENTIALS=</absolute/path/to/gcp_credentials.json> octue --log-level=debug
        start \
        --app-dir=octue/templates/template-child-services/elevation_service \
        --twine=octue/templates/template-child-services/elevation_service/twine.json \
        --config-dir=octue/templates/template-child-services/elevation_service/data/configuration \
        --service-id=8dgd07fa-6bcd-4ec3-a331-69f737a15332
        --delete-topic-and-subscription-on-exit

4. In another terminal window, start the wind speeds service as a server:

.. code-block:: bash

    GOOGLE_APPLICATION_CREDENTIALS=</absolute/path/to/gcp_credentials.json> octue --log-level=debug \
        start \
        --app-dir=octue/templates/template-child-services/wind_speed_service \
        --twine=octue/templates/template-child-services/wind_speed_service/twine.json \
        --config-dir=octue/templates/template-child-services/wind_speed_service/data/configuration \
        --service-id=7b9d07fa-6bcd-4ec3-a331-69f737a15751
        --delete-topic-and-subscription-on-exit

5. In a third terminal window, run the parent app (don't start it as a server):

.. code-block:: bash

    GOOGLE_APPLICATION_CREDENTIALS=</absolute/path/to/gcp_credentials.json> octue --log-level=debug \
        run \
        --app-dir=octue/templates/template-child-services/parent_service \
        --twine=octue/templates/template-child-services/parent_service/twine.json \
        --data-dir=octue/templates/template-child-services/parent_service/data

6. Watch the logs to observe the three services communicate with each other via the cloud in real time. When finished, you will find the output values of the parent in ``octue/templates/template-child-services/parent_service/data/output/values.json``
