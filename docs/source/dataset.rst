.. _dataset:

=======
Dataset
=======

.. admonition:: Definition

    Dataset
        A set of related :doc:`datafiles <datafile>` that exist in the same location, dataset metadata, and helper
        methods.

.. tip::

    Use a dataset if you want to:

    - Group together a set of files that naturally relate to each other e.g. a timeseries that's been split into
      multiple files.
    - Add metadata to it for future sorting and filtering
    - Include it in a :doc:`manifest <manifest>` with other datasets and send them to an Octue service for processing


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
Datafiles in a dataset are stored in a :mod:`FilterSet <octue.resources.filter_containers.FilterSet>`, meaning they
can be easily filtered by any attribute of the datafiles contained e.g. name, extension, ID, timestamp, tags, labels,
size. The filtering syntax is similar to Django's i.e.

.. code-block:: shell

    # Get datafiles that have an attribute that satisfies the filter.
    dataset.files.filter(<datafile_attribute>__<filter>=<value>)

    # Or, if your filter is a simple equality filter:
    dataset.files.filter(<datafile_attribute>=<value>)

Here's an example:

.. code-block:: python

    # Make a dataset.
    dataset = Dataset(
        path="blah",
        files=[
            Datafile(path="my_file.csv", labels=["one", "a", "b" "all"]),
            Datafile(path="your_file.txt", labels=["two", "a", "b", "all"),
            Datafile(path="another_file.csv", labels=["three", "all"]),
        ]
    )

    # Filter it!
    dataset.files.filter(name__starts_with="my")
    >>> <FilterSet({<Datafile('my_file.csv')>})>

    dataset.files.filter(extension="csv")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>

    dataset.files.filter(labels__contains="a")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('your_file.txt')>})>

You can iterate through the filtered files:

.. code-block:: python

    for datafile in dataset.files.filter(labels__contains="a"):
        print(datafile.name)
    >>> 'my_file.csv'
        'your_file.txt'

If there's just one result, get it via the ``one`` method:

.. code-block:: python

    dataset.files.filter(name__starts_with="my").one()
    >>> <Datafile('my_file.csv')>

You can also chain filters or specify them all at the same time - these two examples produce the same result:

.. code-block:: python

    # Chaining multiple filters.
    dataset.files.filter(extension="csv").filter(labels__contains="a")
    >>> <FilterSet({<Datafile('my_file.csv')>})>

    # Specifying multiple filters at once.
    dataset.files.filter(extension="csv", labels__contains="a")
    >>> <FilterSet({<Datafile('my_file.csv')>})>

For the full list of available filters, :doc:`click here <available_filters>`.


Order datasets
--------------
A dataset can also be ordered by any of the attributes of its datafiles:

.. code-block:: python

    dataset.files.order_by("name")
    >>> <FilterList([<Datafile('another_file.csv')>, <Datafile('my_file.csv')>, <Datafile(path="your_file.txt")>])>

The ordering can also be carried out in reverse (i.e. descending order) by passing ``reverse=True`` as a second argument
to the ``order_by`` method.
