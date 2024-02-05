.. _asking_questions:

=========================
Asking services questions
=========================

How to ask a question
=====================
Questions are always asked to a *revision* of a service. You can ask a service a question if you have its
:ref:`SRUID <sruid_definition>`, project name, and the necessary permissions. The question is formed of input values
and/or an input manifest.

.. code-block:: python

    from octue.resources import Child

    child = Child(
        id="my-organisation/my-service:2.1.7",
        backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
    )

    answer = child.ask(
        input_values={"height": 32, "width": 3},
        input_manifest=manifest,
    )

    answer["output_values"]
    >>> {"some": "data"}

    answer["output_manifest"]["my_dataset"].files
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>


.. _using_default_revision_tag:

.. note::

    Not including a service revision tag will cause your question to be sent to the default revision of the service
    (usually the latest deployed version). This is determined by making a request to a `service registry
    <https://django-twined.readthedocs.io/en/latest/>`_ if one or more
    :ref:`registries are defined <using_service_registries>`. If none of the service registries contain an entry for
    this service, a specific service revision tag must be used.

You can also set the following options when you call :mod:`Child.ask <octue.resources.child.Child.ask>`:

- ``children`` - If the child has children of its own (i.e. grandchildren of the parent), this optional argument can be used to override the child's "default" children. This allows you to specify particular versions of grandchildren to use (see :ref:`this subsection below <overriding_children>`).
- ``subscribe_to_logs`` - if true, the child will forward its logs to you
- ``allow_local_files`` - if true, local files/datasets are allowed in any input manifest you supply
- ``handle_monitor_message`` - if provided a function, it will be called on any monitor messages from the child
- ``record_messages_to`` – if given a path to a JSON file, messages received from the parent while it processes the question are saved to it
- ``save_diagnostics`` – must be one of {"SAVE_DIAGNOSTICS_OFF", "SAVE_DIAGNOSTICS_ON_CRASH", "SAVE_DIAGNOSTICS_ON"}; if turned on, allow the input values and manifest (and its datasets) to be saved by the child either all the time or just if the analysis fails
- ``question_uuid`` - if provided, the question will use this UUID instead of a generated one
- ``timeout`` - how long in seconds to wait for an answer (``None`` by default - i.e. don't time out)

If a child raises an exception while processing your question, the exception will always be forwarded and re-raised in
your local service or python session. You can handle exceptions in whatever way you like.

If setting a timeout, bear in mind that the question has to reach the child, the child has to run its analysis on
the inputs sent to it (this most likely corresponds to the dominant part of the wait time), and the answer has to be
sent back to the parent. If you're not sure how long a particular analysis might take, it's best to set the timeout to
``None`` initially or ask the owner/maintainer of the child for an estimate.


Asking multiple questions in parallel
=====================================
You can also ask multiple questions to a service in parallel. By default, if any of the questions fail, an error is
raised and no answers are returned.

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

This method uses multithreading, allowing all the questions to be asked at once instead of one after another.

Options:

- If ``raise_errors=False`` is provided, answers are returned for all successful questions while unraised errors are
  returned for unsuccessful ones
- If ``raise_errors=False`` is provided with ``max_retries > 0``, failed questions are retried up to this number of
  times
- If ``raise_errors=False`` is provided with ``max_retries > 0`` and ``prevent_retries_when`` is set to a list of
  exception types, failed questions are retried except for those whose exception types are in the list
- The maximum number of threads that can be used to ask questions in parallel can be set via the ``max_workers``
  argument. This has no effect on the total number of questions that can be asked via ``Child.ask_multiple``.


Asking a question within a service
==================================
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
          "id": "template-child-services/wind-speed-service:2.1.1",
          "backend": {
            "name": "GCPPubSubBackend",
            "project_name": "my-project"
          }
        },
        {
          "key": "elevation",
          "id": "template-child-services/elevation-service:3.1.9",
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

.. _overriding_children:

Overriding a child's children
=============================
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
            "id": "template-child-services/wind-speed-service:2.1.1",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "octue-sdk-python"
            },
        },
        {
            "key": "elevation",
            "id": "template-child-services/elevation-service:3.1.9",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "octue-sdk-python"
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
                "id": "my/own-service:1.0.0",
                "backend": {
                    "name": "GCPPubSubBackend",
                    "project_name": "octue-sdk-python"
                },
            },
            {
                "key": "elevation",
                "id": "organisation/another-service:0.1.0",
                "backend": {
                    "name": "GCPPubSubBackend",
                    "project_name": "octue-sdk-python"
                },
            },
        ],
    )

Overriding beyond the first generation
--------------------------------------
It's an intentional choice to only go one generation deep with overriding children. If you need to be able to specify a
whole tree of children, grandchildren, and so on, please `upvote this issue.
<https://github.com/octue/octue-sdk-python/issues/528>`_


.. _using_service_registries:

Using a service registry
========================
When asking a question, you can optionally specify one or more `service registries
<https://django-twined.readthedocs.io/en/latest/>`_ to resolve SRUIDs against. This is analogous to specifying a
different ``pip`` index for resolving package names when using ``pip install``. If you don't specify any registries, the
default Octue service registry is used.

Specifying service registries can be useful if:

- You have your own private services that aren't on the default Octue service registry
- You want services from one service registry with the same name as in another service registry to be prioritised

Specifying service registries
-----------------------------
You can specify service registries in two ways:

1. Globally for all questions asked inside a service. In the service configuration (``octue.yaml`` file):

    .. code-block:: yaml

        services:
          - namespace: my-organisation
            name: my-app
            service_registries:
              - name: my-registry
                endpoint: blah.com/services

2. For questions to a specific child, inside or outside a service:

    .. code-block:: python

        child = Child(
            id="my-organisation/my-service:1.1.0",
            backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
            service_registries=[
                {"name": "my-registry", "endpoint": "blah.com/services"},
            ]
        )
