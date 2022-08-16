========================
Troubleshooting services
========================

Getting crash diagnostics
=========================
If a child service you're asking a question to (or a deployed service you manage) fails when processing a question
despite the input data passing twine validation, you can get all the input data, configuration data, and messages sent
by the child (delivery acknowledgement, log records, exceptions, monitor messages, and results) for the question to
check what went wrong using the :mod:`octue get-crash-diagnostics` CLI command. These diagnostics are only available if:

a) You allow them to be when calling :mod:`Child.ask <octue.resources.child.Child.ask>` by setting the
   :mod:`allow_save_diagnostics_data_on_crash` to ``True``
b) The service has a :mod:`crash_diagnostics_cloud_path` specified in its service configuration. This must be a path
   to a location in Google Cloud Storage.

If these requirements are satisfied, the diagnostics will be saved and their location will be sent back to the parent
as a log message. The path will be in the format ``gs://my-bucket/path/to/diagnostics/<analysis-id>``. You
can take this cloud path and run:

.. code-block:: shell

    octue get-crash-diagnostics <cloud-path> <local-path>

to download the diagnostics data to a local directory.
