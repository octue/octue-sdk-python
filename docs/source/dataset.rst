.. _dataset:

=======
Dataset
=======

.. admonition:: Definition

    A set of related :doc:`datafiles <datafile>` that exist in the same location, plus metadata.

Key features
============

Work with local and cloud datasets
----------------------------------
Instantiating a dataset is the same whether it's local or cloud-based.

.. code-block:: python

    from octue.resources import Dataset

    dataset = Dataset(path="path/to/dataset", recursive=True)

    dataset = Dataset(path="gs://my-bucket/path/to/dataset", recursive=True)


Upload a dataset
----------------

.. code-block:: python

    dataset.upload("gs://my-bucket/path/to/upload")


Download a dataset
------------------

.. code-block:: python

    dataset.download("path/to/download")


Easy and expandable custom metadata
-----------------------------------

You can set the following metadata on a dataset:

- Name
- Labels (a set of lowercase strings)
- Tags (a dictionary of key-value pairs)

This metadata is stored locally in a ``.octue`` file in the same directory as the dataset and is used during
``Dataset`` instantiation. It can be accessed like this:

.. code-block:: python

    dataset.name
    >>> "my-dataset"

    dataset.labels
    >>> {"processed"}

    dataset.tags
    >>> {"organisation": "octue", "energy": "renewable"}

You can update the metadata by setting it on the instance while inside the ``Dataset`` context manager.

.. code-block:: python

    with dataset:
        datafile.labels.add("updated")

You can do this outside the context manager too, but you then need to call the update method:

.. code-block:: python

    dataset.labels.add("updated")
    dataset.update_metadata()


Get dataset hashes
------------------
File hashes guarantee you have the right dataset and ensure data consistency. Getting the hash of datasets is simple:

.. code-block:: python

    dataset.hash_value
    >>> 'uvG7TA=='

A datafile's hash is a function of its datafiles' hashes and its own metadata.


Immutable ID
------------
Each dataset has an immutable UUID:

.. code-block:: python

    dataset.id
    >>> '9a1f9b26-6a48-4f2d-be80-468d3270d79c'


Check a dataset's locality
---------------------------

.. code-block:: python

    dataset.exists_locally
    >>> True

    dataset.exists_in_cloud
    >>> False

A dataset can only return ``True`` for one of these at a time.


Filter datasets
---------------
Datafiles in a dataset are stored in a ``FilterSet`` (see :doc:`here <filter_containers>` for more info), meaning they
can be easily filtered by any attribute of the datafile instances contained e.g. name, extension, ID, timestamp, tags,
labels, size. The filtering syntax is similar to Django's.

.. code-block:: python

    dataset = Dataset(
        path="blah",
        files=[
            Datafile(path="blah/my_file.csv", labels=["one", "a", "b" "all"]),
            Datafile(path="blah/your_file.txt", labels=["two", "a", "b", "all"),
            Datafile(path="blah/another_file.csv", labels=["three", "all"]),
        ]
    )

    dataset.files.filter(name__starts_with="my")
    >>> <FilterSet({<Datafile('my_file.csv')>})>

    dataset.files.filter(extension__equals="csv")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>

    dataset.files.filter(labels__contains="a")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('your_file.txt')>})>

You can also chain filters or specify them all at the same time:

.. code-block:: python

    dataset.files.filter(extension__equals="csv").filter(labels__contains="a")
    >>> <FilterSet({<Datafile('my_file.csv')>})>

    dataset.files.filter(extension__equals="csv", labels__contains="a")
    >>> <FilterSet({<Datafile('my_file.csv')>})>
