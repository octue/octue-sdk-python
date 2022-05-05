.. _dataset:

=======
Dataset
=======
A set of related datafiles that exist in the same location, plus metadata. Typically produced by or for one operation in an analysis.

Key features
============

- Group all datafiles for an operation together

- Work with local and cloud datasets in the same way

- Instantiate a dataset from a local or cloud directory (optionally recursively) or add datafiles one by one

  .. code-block::

      dataset = Dataset.from_local_directory("path/to/dataset", recursive=True)

      dataset = Dataset.from_cloud("gs://my-bucket/path/to/dataset", recursive=True)

- Filter the datafiles in a dataset by any of their attributes (name, ID, timestamp, tags, labels, size)

- Upload and download all datafiles in a dataset

- Get a hash for a dataset which combines the datfiles' hashes and the dataset metadata



A ``Dataset`` contains any number of ``Datafiles`` along with the following metadata:

- ``name``
- ``tags``
- ``labels``

The files are stored in a ``FilterSet``, meaning they can be easily filtered according to any attribute of the
:doc:`Datafile <datafile>` instances contained.


--------------------------------
Filtering files in a ``Dataset``
--------------------------------

You can filter a ``Dataset``'s files as follows:

.. code-block:: python

    dataset = Dataset(
        files=[
            Datafile(path="path-within-dataset/my_file.csv", labels=["one", "a", "b" "all"]),
            Datafile(path="path-within-dataset/your_file.txt", labels=["two", "a", "b", "all"),
            Datafile(path="path-within-dataset/another_file.csv", labels=["three", "all"]),
        ]
    )

    dataset.files.filter(name__ends_with=".csv")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>

    dataset.files.filter(labels__contains="a")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('your_file.txt')>})>

You can also chain filters indefinitely, or specify them all at the same time:

.. code-block:: python

    dataset.files.filter(name__ends_with=".csv").filter(labels__contains="a")
    >>> <FilterSet({<Datafile('my_file.csv')>})>

    dataset.files.filter(name__ends_with=".csv", labels__contains="a")
    >>> <FilterSet({<Datafile('my_file.csv')>})>

Find out more about ``FilterSets`` :doc:`here <filter_containers>`, including all the possible filters available for each type of object stored on
an attribute of a ``FilterSet`` member, and how to convert them to primitive types such as ``set`` or ``list``.
