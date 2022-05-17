.. _creating_digital_twins:

======================
Creating digital twins
======================
One of the main features of the Octue SDK is to allow you to easily create digital twins that can accept questions and
return answers. They can run locally on any machine or be deployed to the cloud. Currently:

- The backend communication between twins uses Google Pub/Sub whether they're local or deployed
- The deployment options are Google Cloud Run or Google Dataflow
- The language of the entrypoint must by `python3` (you can call processes using other languages within this though)


Anatomy of an Octue digital twin
================================
An Octue digital twin is defined by the following files (located in the repository root by default):

- An ``octue.yaml`` file defining the structure of the service
- An ``app.py`` entrypoint file
  - This can contain any valid python, including use of any number of external packages or your own subpackages
  - It must contain at least one of the ``octue`` python app interfaces
- A ``twine.json`` file defining your schema for configuration, input, and output values and manifests
- A ``setup.py`` or ``requirements.txt`` file specifying your app's dependencies
- Optionally, a ``Dockerfile`` if your needs exceed the default ``octue`` one. For example, you may need extra system
  dependencies or access to software that isn't installable via ``pip``.
- Optionally, an app configuration JSON file specifying, for example, any children your app depends on

See below for more information on each of these.


octue.yaml
----------
This file defines the basic structure of your digital twin. It must contain at least:

.. code-block:: yaml

    services:
      - name: my-app

It may also need the following key-value pairs:

- ``app_source_path: <path>`` - if your ``app.py`` file is not in the repository root
- ``app_configuration_path: <path>`` - if your app needs an app configuration file that isn't in the repository root
- ``dockerfile_path: <path>`` - if your app needs a ``Dockerfile`` that isn't in the repository root

All paths should be relative to the repository root. Other valid entries can be found in the
:doc:`service configuration constructor </autoapi/octue/configuration/index>`.

.. warning::

    Currently, only one service can be defined per repository, but it must still appear as a list item of the
    "services" key.


app.py
------
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

This allows standardised configuration/input/output of digital twins.


twine.json
----------
This file defines your schema for configuration, input, and output values and manifests. Read more
`here <https://twined.readthedocs.io/en/latest/>`_ and see an example
`here <https://twined.readthedocs.io/en/latest/quick_start_create_your_first_twine.html>`_.


Dependencies file
-----------------
This is a ``setup.py`` file `(read more here) <https://docs.python.org/3/distutils/setupscript.html>`_ or
``requirements.txt`` file `(read more here) <https://learnpython.com/blog/python-requirements-file/>`_ listing all the
python packages your app depends on and the version ranges that will work with your app.


Dockerfile (optional)
---------------------
Octue digital twins run in a Docker container if they are deployed. They can also run locally in a Docker container.
The SDK provides a default ``Dockerfile`` for these purposes that will work for most cases but, if your app requires
non-python/system dependencies (e.g. ``openfast``, ``wget``) or private python packages, you may need to write and
provide your own ``Dockerfile``. If you need help with this, feel free to drop us a message or raise an issue! If you
do provide one, you must specify its path in ``octue.yaml`` under the ``dockerfile_path`` key.


App configuration (optional)
----------------------------
If your app needs any configuration, asks questions to any other Octue digital twins, or produces output
datafiles/datasets, you will need to provide an app configuration. Currently, this can only take the form of JSON file.
It can contain the following keys:

- ``configuration_values``
- ``configuration_manifest``
- ``children``
- ``output_location``

If an app configuration file is provided, its path must be specified in ``octue.yaml`` under the
"app_configuration_path" key.


Template apps
=============
We've created some template apps for you to look at and play around with. We recommend going through them in this order:

1. The `fractal app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-fractal>`_ -
   introduces a basic Octue service that returns output values to its parent.
2. The `using-manifests app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-using-manifests>`_ -
   introduces using a manifest of output datasets to return output files to its parent.
3. The `child-services app template <https://github.com/octue/octue-sdk-python/tree/main/octue/templates/template-child-services>`_ -
   introduces asking questions to child services and using their answers to form an output to return to its parent.
