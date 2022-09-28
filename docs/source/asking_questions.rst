.. _asking_questions:

=========================
Asking services questions
=========================

Octue services
--------------

There's a growing range of live :ref:`services <service_definition>` in the Octue ecosystem that you can ask questions
to and get answers from. Currently, all of them are related to wind energy. Here's a quick glossary of terms before we
tell you more:

.. admonition:: Definitions

    Child
        An Octue service that can be asked a question. This name reflects the tree structure of services (specifically,
        `a DAG <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_) formed by the service asking the question (the
        parent), the child it asks the question to, any children that the child asks questions to as part of forming
        its answer, and so on.

    Parent
        An Octue service that asks a question to another Octue service (a child).

    Asking a question
        Sending data (input values and/or an input manifest) to a child for processing/analysis.

    Receiving an answer
       Receiving data (output values and/or an output manifest) from a child you asked a question to.

    Octue ecosystem
       The set of services running the ``octue`` SDK as their backend. These services guarantee:

       - Defined JSON schemas and validation for input and output data
       - An easy interface for asking them questions and receiving their answers
       - Logs and exceptions (and potentially monitor messages) forwarded to you
       - High availability if deployed in the cloud


How to ask a question
---------------------
You can ask any service a question if you have its service ID, project name, and permissions. The question is formed of
input values and/or an input manifest.

.. code-block:: python

    from octue.resources import Child

    child = Child(
        id="my-organisation/my-service",
        backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
    )

    answer = child.ask(input_values={"height": 32, "width": 3}, input_manifest=manifest)

    answer["output_values"]
    >>> {"some": "data"}

    answer["output_manifest"]["my_dataset"].files
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>


You can also set the following options when you call :mod:`Child.ask <octue.resources.child.Child.ask>`:

- ``subscribe_to_logs`` - if true, the child will forward its logs to you
- ``allow_local_files`` - if true, local files/datasets are allowed in any input manifest you supply
- ``handle_monitor_message`` - if provided a function, it will be called on any monitor messages from the child
- ``question_uuid`` - if provided, the question will use this UUID instead of a generated one
- ``timeout`` - how long in seconds to wait for an answer (``None`` by default - i.e. don't time out)

If a child raises an exception while processing your question, the exception will always be forwarded and re-raised in
your local service or python session. You can handle exceptions in whatever way you like.

If setting a timeout, bear in mind that the question has to reach the child, the child has to run its analysis on
the inputs sent to it (this most likely corresponds to the dominant part of the wait time), and the answer has to be
sent back to the parent. If you're not sure how long a particular analysis might take, it's best to set the timeout to
``None`` initially or ask the owner/maintainer of the child for an estimate.


Asking multiple questions in parallel
-------------------------------------
You can also ask multiple questions to a service in parallel.

.. code-block:: python

    child.ask_multiple(
        {"input_values": {"height": 32, "width": 3}},
        {"input_values": {"height": 12, "width": 10}},
        {"input_values": {"height": 7, "width": 32}},
    )
    >>> [
            {"output_values": {"some": "output"}, "output_manifest": None},
            {"output_values": {"another": "result"}, "output_manifest": None},
            {"output_values": {"different": "result"}, "output_manifest": None},
        ]

This method uses threads, allowing all the questions to be asked at once instead of one after another.


Asking a question within a service
----------------------------------
If you have :doc:`created your own Octue service <creating_services>` and want to ask children questions, you can do
this more easily than above. Children are accessible from the ``analysis`` object by the keys you give them in the
:ref:`app configuration <app_configuration>` file. For example, you can ask an ``elevation`` service a question like
this:

.. code-block:: python

    answer = analysis.children["elevation"].ask(input_values={"longitude": 0, "latitude": 1})

if your app configuration file is:

.. code-block:: json

    {
      "children": [
        {
          "key": "wind_speed",
          "id": "template-child-services/wind-speed-service",
          "backend": {
            "name": "GCPPubSubBackend",
            "project_name": "my-project"
          }
        },
        {
          "key": "elevation",
          "id": "template-child-services/elevation-service",
          "backend": {
            "name": "GCPPubSubBackend",
            "project_name": "my-project"
          }
        }
      ]
    }

and your ``twine.json`` file includes the child keys in its ``children`` field:

.. code-block:: json

    {
        "children": [
            {
                "key": "wind_speed",
                "purpose": "A service that returns the average wind speed for a given latitude and longitude.",
            },
            {
                "key": "elevation",
                "purpose": "A service that returns the elevation for a given latitude and longitude.",
            }
        ]
    }

See the parent service's `app configuration <https://github.com/octue/octue-sdk-python/blob/main/octue/templates/template-child-services/parent_service/app_configuration.json>`_
and `app.py file <https://github.com/octue/octue-sdk-python/blob/main/octue/templates/template-child-services/parent_service/app.py>`_
in the  `child-services app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-child-services>`_
to see this in action.

Overriding a child's children
-----------------------------
If the child you're asking a question to has its own children (static children), you can override these by providing the
IDs of the children you want it to use (dynamic children) to the :mod:`Child.ask <octue.resources.child.Child.ask>`
method. Questions that would have gone to the static children will instead go to the dynamic children. Note that:

- You must provide the children in the same format as they're provided in the :ref:`app configuration <app_configuration>`
- If you override one static child, you must override others, too
- The dynamic children must have the same keys as the static children (so the child knows which service to ask which
  questions)
- You should ensure the dynamic children you provide are compatible with and appropriate for questions from the child
  service

For example, if the child requires these children in its app configuration:

.. code-block:: json

    [
        {
            "key": "wind_speed",
            "id": "template-child-services/wind-speed-service",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "octue-amy"
            },
        },
        {
            "key": "elevation",
            "id": "template-child-services/elevation-service",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "octue-amy"
            },
        }
    ]

then you can override them like this:

.. code-block:: python

    answer = child.ask(
        input_values={"height": 32, "width": 3},
        children=[
            {
                "key": "wind_speed",
                "id": "my/own-service",
                "backend": {
                    "name": "GCPPubSubBackend",
                    "project_name": "octue-amy"
                },
            },
            {
                "key": "elevation",
                "id": "organisation/another-service",
                "backend": {
                    "name": "GCPPubSubBackend",
                    "project_name": "octue-amy"
                },
            },
        ],
    )
