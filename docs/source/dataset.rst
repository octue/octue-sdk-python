.. _dataset:

=======
Dataset
=======
A set of related :doc:`datafiles <datafile>` that exist in the same location, plus metadata. Typically produced by or
needed for one operation in an analysis.

Key features
============

Work with local and cloud datasets
----------------------------------
Instantiating a dataset is the same whether it's local or cloud-based.

.. code-block::

    from octue.resources import Dataset

    dataset = Dataset(path="path/to/dataset", recursive=True)

    dataset = Dataset(path="gs://my-bucket/path/to/dataset", recursive=True)


Upload a dataset
----------------

.. code-block::

    dataset.to_cloud("gs://my-bucket/path/to/upload")


Download a dataset
------------------

.. code-block::

    dataset.download_all_files("path/to/download")


Hash datasets easily
------------------------
File hashes guarantee you have the right dataset. Getting the hash of datasets is simple:

.. code-block:: python

    dataset.hash_value
    >>> 'uvG7TA=='

A datafile's hash is a function of its datafiles' hashes and its own metadata.


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
            Datafile(path="blah/path-within-dataset/my_file.csv", labels=["one", "a", "b" "all"]),
            Datafile(path="blah/path-within-dataset/your_file.txt", labels=["two", "a", "b", "all"),
            Datafile(path="blah/path-within-dataset/another_file.csv", labels=["three", "all"]),
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
