.. _dataset:

=======
Dataset
=======

A ``Dataset`` contains any number of ``Datafiles`` along with the following metadata:

- ``name``
- ``labels``

The files are stored in a ``FilterSet``, meaning they can be easily filtered according to any attribute of the
:doc:`Datafile <datafile>` instances it contains.


--------------------------------
Filtering files in a ``Dataset``
--------------------------------

You can filter a ``Dataset``'s files as follows:

.. code-block:: python

    dataset = Dataset(
        files=[
            Datafile(timestamp=time.time(), path="path-within-dataset/my_file.csv", labels="one a:2 b:3 all"),
            Datafile(timestamp=time.time(), path="path-within-dataset/your_file.txt", labels="two a:2 b:3 all"),
            Datafile(timestamp=time.time(), path="path-within-dataset/another_file.csv", labels="three all"),
        ]
    )

    dataset.files.filter(filter_name="name__ends_with", filter_value=".csv")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>

    dataset.files.filter("labels__contains", filter_value="a:2")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('your_file.txt')>})>

You can also chain filters indefinitely:

.. code-block:: python

    dataset.files.filter(filter_name="name__ends_with", filter_value=".csv").filter("labels__contains", filter_value="a:2")
    >>> <FilterSet({<Datafile('my_file.csv')>})>

Find out more about ``FilterSets`` :doc:`here <filter_containers>`, including all the possible filters available for each type of object stored on
an attribute of a ``FilterSet`` member, and how to convert them to primitive types such as ``set`` or ``list``.
