.. _datafile:

========
Datafile
========

A ``Datafile`` is an Octue type that corresponds to a file, which may exist on your computer or in a cloud store. It has
the following main attributes:

- ``path`` - the path of this file, which may include folders or subfolders, within the dataset.
- ``tags`` - key-value pairs of metadata relevant to this file
- ``labels`` - a space-separated string or iterable of labels relevant to this file
- ``timestamp`` - a posix timestamp associated with the file, in seconds since epoch, typically when it was created but could relate to a relevant time point for the data


-----
Usage
-----

``Datafile`` can be used functionally or as a context manager. When used as a context manager, it is analogous to the
builtin ``open`` function context manager. On exiting the context (``with`` block), it closes the datafile locally and,
if it is a cloud datafile, updates the cloud object with any data or metadata changes.


.. image:: images/datafile_use_cases.png


Example A
---------
**Scenario:** Download a cloud object, calculate Octue metadata from its contents, and add the new metadata to the cloud object

**Starting point:** Object in cloud with or without Octue metadata

**Goal:** Object in cloud with updated metadata

.. code-block:: python

    from octue.resources import Datafile


    project_name = "my-project"
    bucket_name = "my-bucket",
    datafile_path = "path/to/data.csv"

    with Datafile.from_cloud(project_name, bucket_name, datafile_path, mode="r") as (datafile, f):
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
    bucket_name = "my-bucket"
    datafile_path = "path/to/data.csv"

    datafile = Datafile.from_cloud(project_name, bucket_name, datafile_path):

    datafile.timestamp = datetime.now()
    datafile.tags = {"manufacturer": "Vestas", "output": "1MW"}
    datafile.labels = {"new"}

    datafile.to_cloud()  # Or, datafile.update_cloud_metadata()


Example C
---------
**Scenario:** Read in the contents and Octue metadata of an existing cloud object without intent to update it in the cloud

**Starting point:** A cloud object with Octue metadata

**Goal:** Cloud object data (contents) and metadata held locally in local variables

.. code-block:: python

    from octue.resources import Datafile


    project_name = "my-project"
    bucket_name = "my-bucket"
    datafile_path = "path/to/data.csv"

    datafile = Datafile.from_cloud(project_name, bucket_name, datafile_path)

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

    datafile.to_cloud(project_name="my-project", bucket_name="my-bucket", path_in_bucket="path/to/data.dat")


For existing data in an existing local file:

.. code-block:: python

    from octue.resources import Datafile


    tags = {"cleaned": True, "type": "linear"}
    labels = {"Vestas"}

    datafile = Datafile(path="path/to/local/file.dat", tags=tags, labels=labels)
    datafile.to_cloud(project_name="my-project", bucket_name="my-bucket", path_in_bucket="path/to/data.dat")
