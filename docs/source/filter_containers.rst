.. _filter_containers:

=================
Filter containers
=================

A filter container is just a regular python container that has some extra methods for filtering or ordering its
elements. It has the same interface (i.e. attributes and methods) as the primitive python type it inherits from, with
these extra methods:

- ``filter``
- ``order_by``

There are two types of filter containers currently implemented:

- ``FilterSet``
- ``FilterList``

``FilterSets`` are currently used in:

- ``Dataset.files`` to store ``Datafiles``
- ``TagSet.tags`` to store ``Tags``

You can see filtering in action on the files of a ``Dataset`` `here <dataset.rst>`_.


---------
Filtering
---------

Filters are named as ``"<name_of_attribute_to_check>__<filter_action>"``, and any attribute of a member of the
``FilterSet`` whose type or interface is supported can be filtered.
.. code-block:: python
    filter_set = FilterSet(
        {Datafile(path="my_file.csv"), Datafile(path="your_file.txt"), Datafile(path="another_file.csv")}
    )

    filter_set.filter(filter_name="name__ends_with", filter_value=".csv")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>

The following filters are implemented for the following types:

- ``bool``:

    * ``is``
    * ``is_not``

- ``str``:

    * ``is``
    * ``is_not``
    * ``equals``
    * ``not_equals``
    * ``iequals``
    * ``not_iequals``
    * ``lt`` (less than)
    * ``lte`` (less than or equal)
    * ``gt`` (greater than)
    * ``gte`` (greater than or equal)
    * ``contains``
    * ``not_contains``
    * ``icontains`` (case-insensitive contains)
    * ``not_icontains``
    * ``starts_with``
    * ``not_starts_with``
    * ``ends_with``
    * ``not_ends_with``

- ``NoneType``:

    * ``is``
    * ``is_not``

- ``TagSet``:

    * ``is``
    * ``is_not``
    * ``equals``
    * ``not_equals``
    * ``any_tag_contains``
    * ``not_any_tag_contains``
    * ``any_tag_starts_with``
    * ``not_any_tag_starts_with``
    * ``any_tag_ends_with``
    * ``not_any_tag_ends_with``



Additionally, these filters are defined for the following *interfaces* (duck-types). :

- Numbers:

    * ``is``
    * ``is_not``
    * ``equals``
    * ``not_equals``
    * ``lt``
    * ``lte``
    * ``gt``
    * ``gte``

- Iterables:

    * ``is``
    * ``is_not``
    * ``equals``
    * ``not_equals``
    * ``contains``
    * ``not_contains``
    * ``icontains``
    * ``not_icontains``

The interface filters are only used if the type of the attribute of the element being filtered is not found in the first
list of filters.

--------
Ordering
--------
As sets are inherently orderless, ordering a ``FilterSet`` results in a new ``FilterList``, which has the same extra
methods and behaviour as a ``FilterSet``, but is based on the ``list`` type instead - meaning it can be ordered and
indexed etc. A ``FilterSet`` or ``FilterList`` can be ordered by any of the attributes of its members:
.. code-block:: python
    filter_set.order_by("name")
    >>> <FilterList([<Datafile('another_file.csv')>, <Datafile('my_file.csv')>, <Datafile(path="your_file.txt")>])>

The ordering can also be carried out in reverse (i.e. descending order) by passing ``reverse=True`` as a second argument
to the ``order_by`` method.
