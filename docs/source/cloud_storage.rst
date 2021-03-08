.. _cloud_storage:

=============
Cloud storage
=============

Octue SDK is currently integrated with Google Cloud Storage. Later on, we intend to integrate with other cloud
providers, too.

----------------------
Data container classes
----------------------
All of the data container classes in the SDK have a ``to_cloud`` and a ``from_cloud`` method, which handles their
upload/download to/from the cloud, including all relevant metadata from the instance (e.g. tags, ID). Data integrity is
checked before and after upload and download to ensure any data corruption is avoided.

Datafile
--------
Assuming you have an instance of ``Datafile`` called ``datafile``:

.. code-block:: python

    datafile.to_cloud(project_name=<project-name>, bucket_name=<bucket-name>, path_in_bucket=<path/in/bucket>)
    >>> gs://<bucket-name>/<path/in/bucket>

    downloaded_datafile = Datafile.from_cloud(project_name=<project-name>, bucket_name=<bucket-name>, datafile_path=<path/in/bucket>)


Dataset
-------
Datasets are uploaded into an output directory. Within, a directory named after the dataset is created containing the
dataset's datafiles:

.. code-block:: bash

    <output-directory>
    |
    |--- <dataset.name>
         |
         |--- <file-0>
         |--- <file-1>
         |--- <file-2>
         ...

Datasets are downloaded from their directory. Assuming you have an instance of ``Dataset`` called ``dataset``:

.. code-block:: python

    dataset.to_cloud(project_name=<project-name>, bucket_name=<bucket-name>, output_directory=<output-directory>)
    >>> gs://<bucket-name>/<output-directory>/<dataset.name>

    downloaded_dataset = Dataset.from_cloud(project_name=<project-name>, bucket_name=<bucket-name>, path_to_dataset_directory=<output-directory>/<dataset.name>)


Manifest
--------
Manifests are uploaded as a file to the given path. The manifest's datasets are uploaded by default into the same
directory as the manifest file, but this can be disabled by specifying ``store_datasets=False``. Manifests are
downloaded by providing the path to the manifest file. Assuming you have an instance of ``Manifest`` called ``manifest``:

.. code-block:: python

    manifest.to_cloud(project_name=<project-name>, bucket_name=<bucket-name>, path_to_manifest_file=<path/to/manifest/file.json>)
    >>> gs://<bucket-name>/<path/to/manifest/file.json>

    downloaded_manifest = Manifest.from_cloud(project_name=<project-name>, bucket_name=<bucket-name>, path_to_manifest_file=<path/to/manifest/file.json>)


The datasets specified in a manifest can be located anywhere in any bucket as long as their paths are correct in their
``Dataset`` instances. A manifest that takes advantage of this can be created locally with datasets whose paths are set
to cloud locations; it can be uploaded by supplying ``store_datasets=False`` as a parameter to ``Manifest.to_cloud``.

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

A popular method in the ``os.path`` module is ``scandir``, which scans the given directory for files and directories
and allows iteration through them, providing metadata about them. We have implemented a similar method for Google Cloud
storage. A filter can optionally be applied to the blobs (files).

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

1. Provide ``octue.utils.cloud.storage.client.OCTUE_MANAGED_CREDENTIALS`` as the credentials parameter to the
   storage client (this is the default value so can be left unspecified), and an environment variable
   ``GOOGLE_APPLICATION_CREDENTIALS`` containing either the path to or contents of a service account JSON file

2. Provide an instance of ``google.auth.credentials.Credentials`` as the credentials parameter of the storage client,
   opening up a more diverse array of credential possibilities.
