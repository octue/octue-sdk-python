.. _creating_digital_twins:

======================
Creating services
======================
One of the main features of the Octue SDK is to allow you to easily create services that can accept questions and
return answers. They can run locally on any machine or be deployed to the cloud. Currently:

- The backend communication between twins uses Google Pub/Sub whether they're local or deployed
- The deployment options are Google Cloud Run or Google Dataflow
- The language of the entrypoint must by `python3` (you can call processes using other languages within this though)


Anatomy of an Octue service
===========================
An Octue service is defined by the following files (located in the repository root by default):

app.py
------

    .. collapse:: This is the entrypoint into your code - read more...

            ----

        The ``app.py`` file:

        - Can contain any valid python, including use of any number of external packages or your own subpackages
        - Must contain at least one of the ``octue`` python app interfaces

        This is where you write your app. The first requirement is that it must contain exactly one of:

        - A function named ``run`` with the following signature:

          .. code-block:: python

              def run(analysis):
                  """A function that mutates an ``Analysis`` instance.

                  :param octue.resources.Analysis analysis:
                  :return None:
                  """
                  ...

        - A class named ``App`` with the following signature:

          .. code-block:: python

              class App:
                  """A class that takes an ``Analysis`` instance and anything else you like. It can contain any methods you
                  like but it must also have a ``run`` method.

                  :param octue.resources.Analysis analysis:
                  :return None:
                  """

                  def __init__(self, analysis, *args, **kwargs):
                      self.analysis = analysis
                      ...

                  def run(self):
                      """A method that mutates the ``self.analysis`` attribute.

                      :return None:
                      """
                      ...

                  ...

        The second requirement is that your app accesses configuration/input data from and stores output data on the
        ``analysis`` parameter/attribute:

        - Configuration values: ``analysis.configuration_values``
        - Configuration manifest: ``analysis.configuration_manifest``
        - Input values: ``analysis.input_values``
        - Input manifest: ``analysis.input_manifest``
        - Output values: ``analysis.output_values``
        - Output manifest: ``analysis.output_manifest``

        This allows standardised configuration/input/output of services.

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

octue.yaml
----------

    .. collapse:: This defines the structure of the service - read more...

            ----

        This file defines the basic structure of your service. It must contain at least:

        .. code-block:: yaml

            services:
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

App configuration file (optional)
---------------------------------
    .. collapse:: An optional app configuration JSON file specifying, for example, any children your app depends on - read more...

            ----

        If your app needs any configuration, asks questions to any other Octue services, or produces output
        datafiles/datasets, you will need to provide an app configuration. Currently, this can only take the form of JSON file.
        It can contain the following keys:

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


Template apps
=============
We've created some template apps for you to look at and play around with. We recommend going through them in this order:

1. The `fractal app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-fractal>`_ -
   introduces a basic Octue service that returns output values to its parent.
2. The `using-manifests app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-using-manifests>`_ -
   introduces using a manifest of output datasets to return output files to its parent.
3. The `child-services app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-child-services>`_ -
   introduces asking questions to child services and using their answers to form an output to return to its parent.
