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
upload/download to/from the cloud, including all relevant metadata from the instance (e.g. labels, ID). Data integrity is
checked before and after upload and download to ensure any data corruption is avoided.

Datafile
--------
Assuming you have an instance of ``Datafile`` called ``my_datafile``:

.. code-block:: python

    my_datafile.upload("gs://bucket-name/path/in/bucket")
    >>> 'gs://bucket-name/path/in/bucket'

    downloaded_datafile = Datafile(path="gs://bucket-name/path/in/bucket")


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

    my_dataset.upload("gs://bucket-name/output-directory")
    >>> 'gs://bucket-name/output-directory/my_dataset_name'

    downloaded_dataset = Dataset("gs://bucket-name/output-directory/my_dataset_name")


Manifest
--------
Manifests are uploaded as a file to the given path and downloaded by providing the same path. Assuming you have an
instance of ``Manifest`` called ``my_manifest``:

.. code-block:: python

    my_manifest.to_cloud("gs://bucket-name/path/to/manifest/file.json")
    >>> 'gs://bucket-name/path/to/manifest/file.json'

    downloaded_manifest = Manifest.from_cloud("gs://bucket-name/path/to/manifest/file.json")


The datasets specified in a manifest can be located anywhere in any bucket as long as their paths are correct in their
``Dataset`` instances. A manifest that takes advantage of this can be created locally with datasets whose paths are set
to cloud locations.
