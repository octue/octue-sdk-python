.. _datafile:

========
Datafile
========

Key features
============

- Working with a datafile is the same whether it's local or in the cloud. For example, this is how to write to local and
  cloud datafiles:

  .. code-block::

      # Local datafile
      datafile = Datafile("path/to/file.dat")

      with datafile.open("w") as f:
          f.write("Some data")
          datafile.labels.add("processed")

      # Cloud datafile
      datafile = Datafile("gs://my-bucket/path/to/file.dat")

      with datafile.open("w") as f:
          f.write("Some data")
          datafile.labels.add("processed")

- If you're reading or writing a cloud datafile, its content is automatically downloaded so you get low-latency data
  operations

- Downloading is lazy - until you access the cloud datafile contents using the ``open`` context manager, call
  the ``download`` method, or try to get its ``local_path`` attribute, the content isn't downloaded. This makes viewing
  and filtering by the metadata of datasets of cloud datafiles quick.

- Need to run a CLI command on your file as part of your app?

  .. code-block::

      output = subprocess.check_output(["cli-app", "datafile.local_path"])

- Metadata is stored locally for local datafiles and in the cloud for cloud datafiles:
  - ID
  - Timestamp
  - Arbitrary labels (strings)
  - Arbitrary tags (key-value pairs)

  These are read from the cloud object or a local ``.octue`` file on ``Datafile`` instantiation and can be accessed simply:

  .. code-block::

      datafile.id
      >>> '9a1f9b26-6a48-4f2d-be80-468d3270d79b'

      datafile.timestamp
      >>> datetime.datetime(2022, 5, 4, 17, 57, 57, 136739)

      datafile.labels
      >>> {"processed"}

      datafile.tags
      >>> {"organisation": "octue", "energy": "renewable"}

- Metadata can be set on the instance by setting the attributes but the cloud/local metadata stores aren't updated by
  default. You can update them by using the ``open`` context manager or running:

  .. code-block::

      datafile.update_local_metadata()

      datafile.update_cloud_metadata()

- You can upload a local datafile to the cloud without using the ``open`` context manager if you don't need to modify
  its contents:

  .. code-block::

      local_datafile.to_cloud("gs://my-bucket/my_datafile.dat", update_cloud_metadata=True)

- Get the file hash of cloud and local datafiles

  .. code-block::

      dataset.hash_value
      >>> 'mnG7TA=='


More information
================

Check where a datafile exists
-----------------------------

.. code-block:: python

    datafile.exists_locally
    >>> True

    datafile.exists_in_cloud
    >>> False

Representing HDF5 files
-----------------------

.. warning::
    If you want to represent HDF5 files with a ``Datafile``, you must include the extra requirements provided by the
    ``hdf5`` key at installation i.e.

    .. code-block:: shell

        pip install octue[hdf5]

Datafiles existing in the cloud
-------------------------------
To avoid unnecessary data transfer and costs, datafiles that only exist in the cloud are not downloaded locally until
the ``download`` method is called on them or their ``local_path`` property is used for the first time. When either of
these happen, the cloud object is downloaded to a temporary local file. Any changes made to the local file via the
``Datafile.open`` method (which can be used analogously to the python built-in ``open`` function) are synced up with
the cloud object. The temporary file will exist as long as the python session is running. Calling ``download`` again
will not re-download the file as it will be up to date with any changes made locally. However, external changes to the
cloud object will not be synced locally unless the ``local_path`` is set to ``None``, followed by the ``download``
method again.

If you want a cloud object to be permanently downloaded, you can either:

- Set the ``local_path`` property of the datafile to the path you want the object to be downloaded to

  .. code-block:: python

      datafile.local_path = "my/local/path.csv"

- Use the ``download`` method with the ``local_path`` parameter set

  .. code-block:: python

      datafile.download(local_path="my/local/path.csv")

Either way, the datafile will now exist locally as well in the cloud.


Usage examples
==============

The ``Datafile`` class can be used functionally or as a context manager. When used as a context manager, it is analogous
to the builtin ``open`` function context manager. On exiting the context (the ``with`` block), it closes the datafile
locally and, if the datafile also exists in the cloud, updates the cloud object with any data or metadata changes.


.. image:: images/datafile_use_cases.png


Example A
---------
**Scenario:** Download a cloud object, calculate Octue metadata from its contents, and add the new metadata to the cloud object

**Starting point:** Object in cloud with or without Octue metadata

**Goal:** Object in cloud with updated metadata

.. code-block:: python

    from octue.resources import Datafile


    project_name = "my-project"
    path = "gs://my-bucket/path/to/data.csv"

    with Datafile(path, project_name=project_name, mode="r") as (datafile, f):
        data = f.read()
        new_metadata = metadata_calculating_function(data)

        datafile.timestamp = new_metadata["timestamp"]
        datafile.tags = new_metadata["tags"]
        datafile.labels = new_metadata["labels"]


Example B
---------
**Scenario:** Add or update Octue metadata on an existing cloud object *without downloading its content*

**Starting point:** A cloud object with or without Octue metadata

**Goal:** Object in cloud with updated metadata

.. code-block:: python

    from datetime import datetime
    from octue.resources import Datafile


    project_name = "my-project"
    path = "gs://my-bucket/path/to/data.csv"

    datafile = Datafile(path, project_name=project_name)

    datafile.timestamp = datetime.now()
    datafile.tags = {"manufacturer": "Vestas", "output": "1MW"}
    datafile.labels = {"new"}

    datafile.to_cloud()  # Or, datafile.update_cloud_metadata()


Example C
---------
**Scenario:** Read in the data and Octue metadata of an existing cloud object without intent to update it in the cloud

**Starting point:** A cloud object with Octue metadata

**Goal:** Cloud object data (contents) and metadata held locally in local variables

.. code-block:: python

    from octue.resources import Datafile


    project_name = "my-project"
    path = "gs://my-bucket/path/to/data.csv"

    datafile = Datafile(path, project_name=project_name)

    with datafile.open("r") as f:
        data = f.read()

    metadata = datafile.metadata()


Example D
---------
**Scenario:** Create a new cloud object from local data, adding Octue metadata

**Starting point:** A file-like locally (or content data in local variable) with Octue metadata stored in local variables

**Goal:** A new object in the cloud with data and Octue metadata

For creating new data in a new local file:

.. code-block:: python

    from octue.resources import Datafile


    tags = {"cleaned": True, "type": "linear"}
    labels = {"Vestas"}

    with Datafile(path="path/to/local/file.dat", tags=tags, labels=labels, mode="w") as (datafile, f):
        f.write("This is some cleaned data.")

    datafile.to_cloud(project_name="my-project", cloud_path="gs://my-bucket/path/to/data.dat")


For existing data in an existing local file:

.. code-block:: python

    from octue.resources import Datafile


    tags = {"cleaned": True, "type": "linear"}
    labels = {"Vestas"}

    datafile = Datafile(path="path/to/local/file.dat", tags=tags, labels=labels)
    datafile.to_cloud(project_name="my-project", cloud_path="gs://my-bucket/path/to/data.dat")
