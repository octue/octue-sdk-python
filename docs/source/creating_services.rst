=================
Creating services
=================
One of the main features of the Octue SDK is to allow you to easily create services that can accept questions and
return answers. They can run locally on any machine or be deployed to the cloud. Currently:

- The backend communication between twins uses Google Pub/Sub whether they're local or deployed
- The deployment options are Google Cloud Run or Google Dataflow
- The language of the entrypoint must by ``python3`` (you can call processes using other languages within this though)


Anatomy of an Octue service
===========================
An Octue service is defined by the following files (located in the repository root by default).

app.py
------
This is the entrypoint into your code - :doc:`read more here <creating_apps>`.


twine.json
----------
This file defines the schema for the service's configuration, input, and output data. Read more
`here <https://twined.readthedocs.io/en/latest/>`_ and see an example
`here <https://twined.readthedocs.io/en/latest/quick_start_create_your_first_twine.html>`_.

Dependencies file
-----------------
A file specifying your app's dependencies. This is a `setup.py file <https://docs.python.org/3/distutils/setupscript.html>`_,
a `requirements.txt file <https://learnpython.com/blog/python-requirements-file/>`_, or a
`pyproject.toml file <https://python-poetry.org/docs/pyproject/>`_ listing all the python packages your app depends on
and the version ranges that are supported.

.. _octue_yaml:

octue.yaml
----------

    .. collapse:: This describes the service configuration - read more...

            ----

        This file defines the basic structure of your service. It must contain at least:

        .. code-block:: yaml

            services:
              - namespace: my-organisation
                name: my-app

        It may also need the following key-value pairs:

        - ``app_source_path: <path>`` - if your ``app.py`` file is not in the repository root
        - ``app_configuration_path: <path>`` - if your app needs an app configuration file that isn't in the repository root
        - ``dockerfile_path: <path>`` - if your app needs a ``Dockerfile`` that isn't in the repository root

        All paths should be relative to the repository root. Other valid entries can be found in the
        :mod:`ServiceConfiguration <octue.configuration.ServiceConfiguration>` constructor.

        .. warning::

            Currently, only one service can be defined per repository, but it must still appear as a list item of the
            "services" key. At some point, it will be possible to define multiple services in one repository.

.. _app_configuration:

App configuration file (optional)
---------------------------------

    .. collapse:: An optional app configuration JSON file specifying, for example, any children your app depends on - read more...

            ----

        If your app needs any configuration, asks questions to any other Octue services, or produces output
        datafiles/datasets, you will need to provide an app configuration. Currently, this must take the form of a JSON
        file. It can contain the following keys:

        - ``configuration_values``
        - ``configuration_manifest``
        - ``children``
        - ``output_location``

        If an app configuration file is provided, its path must be specified in ``octue.yaml`` under the
        "app_configuration_path" key.

        See the :mod:`AppConfiguration <octue.configuration.AppConfiguration>` constructor for more information.

Dockerfile (optional)
---------------------

    .. collapse:: Provide this if your needs exceed the default Octue Dockerfile - read more...

            ----

        Octue services run in a Docker container if they are deployed. They can also run this way locally. The SDK
        provides a default ``Dockerfile`` for these purposes that will work for most cases:

        - For deploying to `Google Cloud Run <https://github.com/octue/octue-sdk-python/blob/main/octue/cloud/deployment/google/cloud_run/Dockerfile>`_
        - For deploying to `Google Dataflow <https://github.com/octue/octue-sdk-python/blob/main/octue/cloud/deployment/google/dataflow/Dockerfile>`_

        However, you may need to write and provide your own ``Dockerfile`` if your app requires:

        - Non-python or system dependencies (e.g. ``openfast``, ``wget``)
        - Python dependencies that aren't installable via ``pip``
        - Private python packages

        Here are two examples of a custom ``Dockerfile`` that use different base images:

        - `A TurbSim service <https://github.com/aerosense-ai/turbsim-service/blob/main/Dockerfile>`_
        - `An OpenFAST service <https://github.com/aerosense-ai/openfast-service/blob/main/Dockerfile>`_

        If you do provide one, you must specify its path in ``octue.yaml`` under the ``dockerfile_path`` key.

        As always, if you need help with this, feel free to drop us a message or raise an issue!


Where to specify the namespace, name, and revision tag
------------------------------------------------------
See :ref:`here <service_naming>` for service naming requirements.

**Namespace**

- Required: yes
- Set in:

  - ``octue.yaml``
  - ``OCTUE_SERVICE_NAMESPACE`` environment variable (takes priority)

**Name**

- Required: yes
- Set in:

  - ``octue.yaml``
  - ``OCTUE_SERVICE_NAME`` environment variable (takes priority)

**Revision tag**

- Required: no
- Default: a random "coolname" (e.g. ``hungry-hippo``)
- Set in:

  - ``OCTUE_SERVICE_REVISION_TAG`` environment variable
  - If using ``octue start`` command, the ``--revision-tag`` option (takes priority)


Template apps
=============
We've created some template apps for you to look at and play around with. We recommend going through them in this order:

1. The `fractal app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-fractal>`_ -
   introduces a basic Octue service that returns output values to its parent.
2. The `using-manifests app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-using-manifests>`_ -
   introduces using a manifest of output datasets to return output files to its parent.
3. The `child-services app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-child-services>`_ -
   introduces asking questions to child services and using their answers to form an output to return to its parent.


Deploying services automatically
================================
Automated deployment with Octue means:

- Your service runs in Google Cloud, ready to accept questions from and return answers to other services.
- You don't need to do anything to update your deployed service with new code changes - the service simply gets rebuilt
  and re-deployed each time you push a commit to your ``main`` branch, or merge a pull request into it (other branches
  and deployment strategies are available, but this is the default).
- Serverless is the default - your service only runs when questions from other services are sent to it, meaning there
  is no cost to having it deployed but not in use.

To enable automated deployments, contact us so we can create a Google Cloud Build trigger linked to your git repository.
This requires no work from you apart from authorising the connection to GitHub (or another git provider).

If you want to deploy services yourself, see :doc:`here <deploying_services>`.
