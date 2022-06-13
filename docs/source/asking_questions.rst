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
        An Octue service that can be asked a question. This name reflects the tree structure of services formed by the
        service asking the question (the parent), the child it asks the question to, any children that the child asks
        questions to as part of forming its answer, and so on.

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
        name="wind_speed",
        id="4acbf2aa-54ce-4bae-b473-a062e21b3d57",
        backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
    )

    answer = child.ask(input_values={"height": 32, "width": 3}, input_manifest=manifest)

    answer["output_values"]
    >>> {"some": "data"}

    answer["output_manifest"]["my_dataset"].files
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>


You can also set the following options when you call ``ask``:

- ``subscribe_to_logs`` - if true, the child will forward its logs to you
- ``allow_local_files`` - if true, local files/datasets are allowed in any input manifest you supply
- ``handle_monitor_message`` - if provided a callable, this will handle any monitor messages from the child
- ``question_uuid`` - if provided, the question will use this UUID instead of a generated one
- ``timeout`` - how long in seconds to wait for an answer


Asking a question within a service
----------------------------------
If you have :doc:`created your own Octue service <creating_services>` and want to ask children questions, you can do
this more easily than above. Children are accessible from the ``analysis`` object by the keys you give them in the
:ref:`app configuration <app_configuration>` file. For example, if your configuration file is:

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

then you can ask the ``elevation`` service a question like this:

.. code-block:: python

    answer = analysis.children["elevation"].ask(input_values={"longitude": 0, "latitude": 1})

See the parent service's `app configuration <https://github.com/octue/octue-sdk-python/blob/main/octue/templates/template-child-services/parent_service/app_configuration.json>`_
and `app.py file <https://github.com/octue/octue-sdk-python/blob/main/octue/templates/template-child-services/parent_service/app.py>`_
in the  `child-services app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-child-services>`_
to see this in action.
