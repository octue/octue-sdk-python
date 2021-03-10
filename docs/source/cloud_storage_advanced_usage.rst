.. _cloud_storage_advanced_usage:

============================
Cloud storage advanced usage
============================

-----------------
Files and strings
-----------------

Any file or string can also be uploaded as a file to Google Cloud storage. Any relevant metadata can be attached
to these files as long as it's provided in dictionary form where both the keys and values are JSON-serialisable. The
methods to do this are accessed via the ``GoogleCloudStorageClient``. A timeout in seconds can optionally be supplied
to any of these methods.

**Uploading**

.. code-block:: python

    from octue.utils.cloud.storage.client import GoogleCloudStorageClient


    storage_client = GoogleCloudStorageClient(project_name=<project-name>)

    storage_client.upload_file(
        local_path=<path/to/file>,
        bucket_name=<bucket-name>,
        path_in_bucket=<path/to/file/in/bucket>,
        metadata={"tags": ["blah", "glah", "jah"], "cleaned": True, "id": 3}
    )

    storage_client.upload_from_string(
        string='[{"height": 99, "width": 72}, {"height": 12, "width": 103}]',
        bucket_name=<bucket-name>,
        path_in_bucket=<path/to/file/in/bucket>,
        metadata={"tags": ["dimensions"], "cleaned": True, "id": 96}
    )

**Downloading**

.. code-block:: python

    storage_client.download_to_file(
        bucket_name=<bucket-name>,
        path_in_bucket=<path/to/file/in/bucket>,
        local_path=<path/to/file>
    )

    storage_client.download_as_string(
        bucket_name=<bucket-name>,
        path_in_bucket=<path/to/file/in/bucket>,
    )
    >>> '[{"height": 99, "width": 72}, {"height": 12, "width": 103}]'


**Getting metadata**

.. code-block:: python

    storage_client.get_metadata(
        bucket_name=<bucket-name>,
        path_in_bucket=<path/to/file/in/bucket>,
    )
    >>> {"tags": ["dimensions"], "cleaned": True, "id": 96}


**Deleting**

Files can also be deleted.

.. code-block:: python

    storage_client.delete(bucket_name=<bucket-name>, path_in_bucket=<path/to/file/in/bucket>)


**Scanning cloud directories**

A popular method in the ``os`` module is ``scandir``, which scans the given directory for files and directories
and allows iteration through them, providing their metadata (but not their file contents, which you can separately
request while using the method). We have implemented a similar method for Google Cloud storage. A filter can optionally
be applied to the blobs (files).

.. code-block:: python

    file_filter=lambda blob: blob.name.endswith(".json")

    for file in storage_client.scandir(bucket_name=<bucket-name>, path_in_bucket=<path/to/dir>, filter=file_filter):
        print(blob.name)
    >>> path/to/dir/file.json
        path/to/dir/another_file.json
        path/to/dir/blah.json


-------------------
storage.path module
-------------------
The ``os.path`` module is very useful for working with paths on a regular filesystem, but the paths it makes are not
compatible with Google Cloud Storage when run on some systems. We have implemented analogues of several of the most
used methods here in the ``octue.utils.cloud.storage.path`` module.


-----------
Credentials
-----------
To use any of the methods above, valid Google Cloud Storage credentials are needed. There are a few ways to
provide them:

1. Provide no credentials to the storage client (which by default uses Octue-managed credentials from the environment)
   while providing an environment variable ``GOOGLE_APPLICATION_CREDENTIALS`` containing either the path to or contents
   of a service account JSON file.

2. Advanced users of Google Cloud may choose to provide an instance of ``google.auth.credentials.Credentials`` as the
   credentials parameter of the storage client, opening up a more diverse array of credential possibilities.
