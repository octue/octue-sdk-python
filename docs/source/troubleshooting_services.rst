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
- Inputs to and messages received in answer to each question the service asked its children (if it has any). These are
  stored in the order the questions were asked.

.. important::

    For this feature to be enabled, the child must have the ``diagnostics_cloud_path`` field in its service
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

.. _test_fixtures_from_crash_diagnostics:

Creating test fixtures from crash diagnostics
=============================================
You can create test fixtures directly from crash diagnostics, allowing you to recreate the exact conditions that caused
your service to fail.

.. code-block:: python

    from unittest.mock import patch

    from octue import Runner
    from octue.utils.testing import load_test_fixture_from_crash_diagnostics


     (
         configuration_values,
         configuration_manifest,
         input_values,
         input_manifest,
         child_emulators,
     ) = load_test_fixture_from_crash_diagnostics(path="path/to/downloaded/crash/diagnostics")

    # You can explicitly specify your children here as shown or
    # read the same information in from your app configuration file.
    children = [
        {
            "key": "my_child",
            "id": "octue/my-child-service:2.1.0",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "my-project",
            }
        },
        {
            "key": "another_child",
            "id": "octue/another-child-service:2.1.0",
            "backend": {
                "name": "GCPPubSubBackend",
                "project_name": "my-project",
            }
        }
    ]

    runner = Runner(
        app_src="path/to/directory_containing_app",
        twine=os.path.join(app_directory_path, "twine.json"),
        children=children,
        configuration_values=configuration_values,
        configuration_manifest=configuration_manifest,
        service_id="your-org/your-service:2.1.0",
    )

    with patch("octue.runner.Child", side_effect=child_emulators):
        analysis = runner.run(input_values=input_values, input_manifest=input_manifest)


Disabling crash diagnostics
===========================
When asking a question to a child, parents can disable crash diagnostics upload in the child on a question-by-question
basis by setting ``save_diagnostics`` to ``"SAVE_DIAGNOSTICS_OFF"`` in :mod:`Child.ask <octue.resources.child.Child.ask>`.
For example:

.. code-block:: python

    child = Child(
        id="my-organisation/my-service:2.1.0",
        backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
    )

    answer = child.ask(
        input_values={"height": 32, "width": 3},
        save_diagnostics="SAVE_DIAGNOSTICS_OFF",
    )
