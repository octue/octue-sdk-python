.. _child_services:

==============
Child services
==============
When a Twine file is written, there is the option to include children (i.e. child services or child digital twins) so
the main or parent service can communicate with them to ask them "questions". A question is a set of input
values and/or an input manifest in the form the child's Twine specifies. When a question is asked, the parent can expect
an answer, which is a set of output values and/or an output manifest, again in the form specified by the child's Twine.

There can be:

- Any number of children
- Any number of questions asked to each child

Further, a child can have its own children that it asks questions to. There is no limit to this as long as the tree of
services forms a directed acyclical graph (DAG) - i.e. there are no loops and no children ask their parents any
questions.


-------------------------
Example usage in your app
-------------------------

Assuming you have specified which children you would like to use in your ``twine.json`` file (see below for an example),
you can ask children questions in your ``app.py`` file as follows:

.. code-block:: python

    answer_1 = analysis.children["child_1"].ask(input_values=analysis.input_values, timeout=None)
    answer_2 = analysis.children["child_2"].ask(input_values=analysis.input_values, timeout=None)

    >>> answer_1
    {
        'output_values': <output values in form specified by child twine.json>,
        'output_manifest': <output manifest in form specified by child twine.json>
    }


A timeout (measured in seconds) can be set for how long you are willing to wait for an answer, but bear in mind that the
question has to reach the child, the child has to run its own analysis on the inputs sent to it (this most likely
corresponds to the dominant part of the wait time), and the answer has to be send back to the parent. If you are not
sure how long a particular analysis might take, it's best to set the timeout to ``None`` or ask the owner/maintainer of
the child for an estimate.


--------
Backends
--------

The backend specifies which method of communication the child uses (e.g. Google Cloud Pub/Sub), as well as providing
pointers to the credentials and any other parameters necessary to access it. Each child must have its backend
specified explicitly, even if all children use the same one. This is to support the use case where each child uses a
different backend.

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
                "project_name": "<google_cloud_project_name>",
                "credentials_filename": "<absolute/path/to/credentials_file.json>"
            }
        },
        {
            "key": "elevation",
            "id": "8dgd07fa-6bcd-4ec3-a331-69f737a15332",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "<google_cloud_project_name>",
                "credentials_filename": "<absolute/path/to/credentials_file.json>"
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
                "filters": "tags:wind_speed"
            },
            {
                "key": "elevation",
                "purpose": "A service that returns the elevation for a given latitude and longitude.",
                "notes": "Some notes.",
                "filters": "tags:elevation"
            }
        ],
        ...
    }


----------------------------
Starting a child as a server
----------------------------

For a service to ask another service questions, the askee must already be running as a server. The person/organisation
responsible for the askee must start the askee as a server if it is to be accessible to questions.

To start a service as a server, the command line interface (CLI) can be used:

.. code-block:: bash

    octue-app start \
    --app-dir=<path/to/app_directory> \
    --twine=<path/to/twine.json> \
    --config-dir=<path/to/configuration> \
    --service-id=<UUID of service>

The service ID must be the UUID of the service as registered with Octue.
