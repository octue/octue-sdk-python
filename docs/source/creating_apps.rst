.. _creating_apps:

app.py file
===========
The ``app.py`` file is, as you might expect, the entrypoint to your app. It can contain any valid python including
imports and use of any number of external packages or internal/local packages.

Structure
---------
The ``app.py`` file must contain exactly one of the ``octue`` python app interfaces to serve as the entrypoint to your
code. These take a single :mod:`Analysis <octue.resources.analysis.Analysis>` instance as a parameter or attribute:

- **Option 1:** A function named ``run`` with the following signature:

    .. code-block:: python

        def run(analysis):
            """A function that uses input and configuration from an ``Analysis``
            instance and stores any output values and output manifests on it.
            It shouldn't return anything.

            :param octue.resources.Analysis analysis:
            :return None:
            """
            ...

- **Option 2:** A class named ``App`` with the following signature:

    .. code-block:: python

        class App:
            """A class that takes an ``Analysis`` instance and any number of
            other parameters in its constructor. It can have any number of
            methods but must always have a ``run`` method with the signature
            shown below.

            :param octue.resources.Analysis analysis:
            :return None:
            """

            def __init__(self, analysis):
                self.analysis = analysis
                ...

            def run(self):
                """A method that that uses input and configuration from an
                ``Analysis`` instance and stores any output values and
                output manifests on it. It shouldn't return anything.

                :return None:
                """
                ...

            ...


Accessing inputs and storing outputs
------------------------------------
Your app must access configuration and input data from and store output data on the :mod:`analysis <octue.resources.analysis.Analysis>`
parameter (for function-based apps) or attribute (for class-based apps). This allows standardised
configuration/input/output validation against the twine and interoperability of all Octue services while leaving you
freedom to do any kind of computation. To access the data, use the following attributes on the ``analysis``
parameter/attribute:

- Configuration values: ``analysis.configuration_values``
- Configuration manifest: ``analysis.configuration_manifest``
- Input values: ``analysis.input_values``
- Input manifest: ``analysis.input_manifest``
- Output values: ``analysis.output_values``
- Output manifest: ``analysis.output_manifest``
