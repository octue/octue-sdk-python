========================
Troubleshooting services
========================

Crash diagnostics
=================
Services save the following data to the cloud if they crash while processing a question:

- Input values
- Input manifest and datasets
- Child configuration values
- Child configuration manifest and datasets
- Inputs to and messages received in answer to each question the service asked its children (if it has any)

.. important::

    For this feature to be enabled, the child must have the ``crash_diagnostics_cloud_path`` field in its service
    configuration (:ref:`octue.yaml <octue_yaml>` file) set to a Google Cloud Storage path.

Accessing crash diagnostics
===========================
In the event of a crash, the service will upload the crash diagnostics and send the upload path to the parent as a log
message. A user with credentials to access this path can use the ``octue`` CLI to retrieve the crash diagnostics data:

.. code-block:: shell

    octue get-crash-diagnostics <cloud-path>

More information on the command:

.. code-block::

    >>> octue get-crash-diagnostics -h

    Usage: octue get-crash-diagnostics [OPTIONS] CLOUD_PATH

      Download crash diagnostics for an analysis from the given directory in
      Google Cloud Storage. The cloud path should end in the analysis ID.

      CLOUD_PATH: The path to the directory in Google Cloud Storage containing the
      diagnostics data.

    Options:
      --local-path DIRECTORY  The path to a directory to store the directory of
                              diagnostics data in. Defaults to the current working
                              directory.
      --download-datasets     If provided, download any datasets from the crash
                              diagnostics and update their paths in their
                              manifests to the new local paths.
      -h, --help              Show this message and exit.



Disabling crash diagnostics
===========================
When asking a question to a child, parents can disable crash diagnostics upload in the child on a question-by-question
basis by setting ``allow_save_diagnostics_data_on_crash`` to ``False`` in :mod:`Child.ask <octue.resources.child.Child.ask>`.
For example:

.. code-block:: python

    child = Child(
        id="my-organisation/my-service:latest",
        backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
    )

    answer = child.ask(
        input_values={"height": 32, "width": 3},
        allow_save_diagnostics_data_on_crash=False,
    )
