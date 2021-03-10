.. _cloud_storage:

=============
Cloud storage
=============

Octue SDK is currently integrated with Google Cloud Storage. Later on, we intend to integrate with other cloud
providers, too. If you need a certain cloud provider and are interested in contributing to or sponsoring its development
in Octue SDK, please join the discussion `in this issue. <https://github.com/octue/octue-sdk-python/issues/108>`_

----------------------
Data container classes
----------------------
All of the data container classes in the SDK have a ``to_cloud`` and a ``from_cloud`` method, which handles their
upload/download to/from the cloud, including all relevant metadata from the instance (e.g. tags, ID). Data integrity is
checked before and after upload and download to ensure any data corruption is avoided.

Datafile
--------
Assuming you have an instance of ``Datafile`` called ``my_datafile``:

.. code-block:: python

    my_datafile.to_cloud(project_name=<project-name>, bucket_name=<bucket-name>, path_in_bucket=<path/in/bucket>)
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

Datasets are downloaded from their directory. Assuming you have an instance of ``Dataset`` called ``my_dataset``:

.. code-block:: python

    my_dataset.to_cloud(project_name=<project-name>, bucket_name=<bucket-name>, output_directory=<output-directory>)
    >>> gs://<bucket-name>/<output-directory>/<my_dataset.name>

    downloaded_dataset = Dataset.from_cloud(project_name=<project-name>, bucket_name=<bucket-name>, path_to_dataset_directory=<output-directory>/<my_dataset.name>)


Manifest
--------
Manifests are uploaded as a file to the given path. The manifest's datasets are uploaded by default into the same
directory as the manifest file, but this can be disabled by specifying ``store_datasets=False``. Manifests are
downloaded by providing the path to the manifest file. Assuming you have an instance of ``Manifest`` called ``my_manifest``:

.. code-block:: python

    my_manifest.to_cloud(project_name=<project-name>, bucket_name=<bucket-name>, path_to_manifest_file=<path/to/manifest/file.json>)
    >>> gs://<bucket-name>/<path/to/manifest/file.json>

    downloaded_manifest = Manifest.from_cloud(project_name=<project-name>, bucket_name=<bucket-name>, path_to_manifest_file=<path/to/manifest/file.json>)


The datasets specified in a manifest can be located anywhere in any bucket as long as their paths are correct in their
``Dataset`` instances. A manifest that takes advantage of this can be created locally with datasets whose paths are set
to cloud locations; it can be uploaded by supplying ``store_datasets=False`` as a parameter to ``Manifest.to_cloud``.
