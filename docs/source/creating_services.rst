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

    .. collapse:: This is the entrypoint into your code - read more...

            ----

        This is where you write your app. The ``app.py`` file can contain any valid python, including import and use of
        any number of external packages or your own subpackages. It has only two requirements:

        1. It must contain exactly one of the ``octue`` python app interfaces that serve as an entrypoint to your code.
           These take a single :mod:`Analysis <octue.resources.analysis.Analysis>` instance:

            - **Option 1:** A function named ``run`` with the following signature:

                .. code-block:: python

                    def run(analysis):
                        """A function that uses input and configuration from an ``Analysis`` instance and stores any
                        output values and output manifests on it.

                        :param octue.resources.Analysis analysis:
                        :return None:
                        """
                        ...

            - **Option 2:** A class named ``App`` with the following signature:

                .. code-block:: python

                    class App:
                        """A class that takes an ``Analysis`` instance and anything else you like. It can contain any
                        methods you like but it must also have a ``run`` method.

                        :param octue.resources.Analysis analysis:
                        :return None:
                        """

                        def __init__(self, analysis, *args, **kwargs):
                            self.analysis = analysis
                            ...

                        def run(self):
                            """A method that that uses input and configuration from an ``Analysis`` instance and stores
                            any output values and output manifests on it.

                            :return None:
                            """
                            ...

                        ...

        2. It must access configuration/input data from and store output data on the :mod:`analysis
           <octue.resources.analysis.Analysis>` parameter/attribute:

        - Configuration values: ``analysis.configuration_values``
        - Configuration manifest: ``analysis.configuration_manifest``
        - Input values: ``analysis.input_values``
        - Input manifest: ``analysis.input_manifest``
        - Output values: ``analysis.output_values``
        - Output manifest: ``analysis.output_manifest``

        This allows standardised configuration/input/output for services while allowing you to do anything you like with
        the input data to produce the output data.

twine.json
----------

    .. collapse:: This is your schema for configuration, input, and output values and manifests - read more...

            ----

        This file defines your schema for configuration, input, and output values and manifests. Read more
        `here <https://twined.readthedocs.io/en/latest/>`_ and see an example
        `here <https://twined.readthedocs.io/en/latest/quick_start_create_your_first_twine.html>`_.

Dependencies file
-----------------

    .. collapse:: A file specifying your app's dependencies - read more...

            ----

        This is a ``setup.py`` file `(read more here) <https://docs.python.org/3/distutils/setupscript.html>`_ or
        ``requirements.txt`` file `(read more here) <https://learnpython.com/blog/python-requirements-file/>`_ listing all the
        python packages your app depends on and the version ranges that will work with your app.

.. _octue_yaml:

octue.yaml
----------

    .. collapse:: This describes the service configuration - read more...

            ----

        This file defines the basic structure of your service. It must contain at least:

        .. code-block:: yaml

            services:
              - namespace: my-organisation
              - name: my-app

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

Naming services
===============

.. admonition:: Definitions

    Service revision
        A specific instance of an Octue service that can be individually addressed. The revision could correspond to a
        version of the service, a dynamic development branch for it, or a deliberate duplication or variation of it.

    Service revision unique identifier (SRUID)
        The combination of a service's namespace, name, and revision tag that uniquely identify it. For example,
        ``octue/my-service:1.3.0`` where the namespace is ``octue``, the name is ``my-service``, and the revision
        tag is ``1.3.0``.

    Service namespace
        The group to which the service belongs e.g. your name or your organisation's name. If in doubt, use the GitHub
        handle of the user or organisation publishing the services.

        Namespaces must be lower kebab case (i.e. they may contain the letters [a-z], numbers [0-9], and hyphens [-]).
        They may not begin or end with hyphens.

    Service name
        A name to uniquely identify the service within its namespace. This usually corresponds to the name of the GitHub
        repository for the service. Names must be lower kebab case (i.e. they may contain the letters [a-z],
        numbers [0-9] and hyphens [-]). They may not begin or end with hyphens.

    Service revision tag
        A tag that uniquely identifies a particular revision of a service. The revision tag could correspond to a commit
        hash like ``a3eb45``, a release number like ``0.12.4``, a branch name (e.g. ``development``), a particular
        environment the service is deployed in (e.g. ``production``), or a combination like ``0.12.4-production``. Tags
        may contain lowercase and uppercase letters, numbers, underscores, periods, and hyphens but can't start with a
        period or a dash. They can contain a maximum of 128 characters. These requirements are the same as the `Docker
        tag format <https://docs.docker.com/engine/reference/commandline/tag/>`_.

    Service ID
        A service ID can be used to ask a question to a service without specifying a specific revision of it. This makes
        a service ID almost the same as an SRUID, but less specific. This enables asking questions to, for example, the
        service ``octue/my-service`` and automatically having them routed to its latest revision. Note that this will
        be a future feature - currently, you will still be required to also provide a revision tag (i.e. a full SRUID).

Where to specify the namespace, name, and revision tag
------------------------------------------------------
The name and namespace are read from the service's ``octue.yaml`` file. However, to make deployment to particular
environments easier, they can alternatively be specified via the following environment variables:

- ``OCTUE_SERVICE_NAMESPACE``
- ``OCTUE_SERVICE_NAME``

These environment variables take precedence over the values in ``octue.yaml``.

Revision tags are specified differently - they're how you keep service revisions unique, so it doesn't make sense to
hard-code them in ``octue.yaml``. If the revision tag is not specified in the ``OCTUE_SERVICE_REVISION_TAG`` environment
variable or, if using the ``octue start`` CLI command, via the ``--revision-tag`` CLI option, a "coolname" tag (e.g.
``hungry-hippo``) will be generated to ensure uniqueness of the resources used. If the CLI option is provided, it takes
precedence over the ``OCTUE_SERVICE_REVISION_TAG`` environment variable.


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
