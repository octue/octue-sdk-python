.. _dataset:

=======
Dataset
=======

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
