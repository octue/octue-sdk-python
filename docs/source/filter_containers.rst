.. _filter_containers:

=================
Filter containers
=================

A filter container is just a regular python container that has some extra methods for filtering and ordering its
elements. It has the same interface (i.e. attributes and methods) as the primitive python type it inherits from, with
these extra methods:

- ``filter``
- ``order_by``

There are three types of filter containers currently implemented:

- ``FilterSet``
- ``FilterList``
- ``FilterDict``

``FilterSets`` are currently used in ``Dataset.files`` to store ``Datafiles`` and make them filterable, which is useful
for dealing with a large number of datasets, while ``FilterList`` is returned when ordering any filter container.

You can see an example of filtering of a ``Dataset``'s files :doc:`here <dataset>`.


---------
Filtering
---------

Key points:

* Any attribute of a member of a filter container whose type or interface is supported can be used when filtering
* Filters are named as ``"<name_of_attribute_to_check>__<filter_action>"``
* Multiple filters can be specified at once for chained filtering
* ``<name_of_attribute_to_check>`` can be a single attribute name or a double-underscore-separated string of nested attribute names
* Nested attribute names work for real attributes as well as dictionary keys (in any combination and to any depth)

.. code-block:: python

    filter_set = FilterSet(
        {
            Datafile(path="my_file.csv", cluster=0, tags={"manufacturer": "Vestas"}),
            Datafile(path="your_file.txt", cluster=1, tags={"manufacturer": "Vergnet"}),
            Datafile(path="another_file.csv", cluster=2, tags={"manufacturer": "Enercon"})
        }
    )

    # Single filter, non-nested attribute.
    filter_set.filter(name__ends_with=".csv")
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>

    # Two filters, non-nested attributes.
    filter_set.filter(name__ends_with=".csv", cluster__gt=1)
    >>> <FilterSet({<Datafile('another_file.csv')>})>

    # Single filter, nested attribute.
    filter_set.filter(tags__manufacturer__startswith("V"))
    >>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('your_file.csv')>})>


These filters are currently available for the following types:

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

- ``LabelSet``:

    * ``is``
    * ``is_not``
    * ``equals``
    * ``not_equals``
    * ``contains``
    * ``not_contains``
    * ``any_label_contains``
    * ``not_any_label_contains``
    * ``any_label_starts_with``
    * ``not_any_label_starts_with``
    * ``any_label_ends_with``
    * ``not_any_label_ends_with``


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
As sets and dictionaries are inherently orderless, ordering any filter container results in a new ``FilterList``, which
has the same methods and behaviour but is based on ``list`` instead, meaning it can be ordered and indexed etc. A
filter container can be ordered by any of the attributes of its members:

.. code-block:: python

    filter_set.order_by("name")
    >>> <FilterList([<Datafile('another_file.csv')>, <Datafile('my_file.csv')>, <Datafile(path="your_file.txt")>])>

    filter_set.order_by("cluster")
    >>> <FilterList([<Datafile('my_file.csv')>, <Datafile('your_file.csv')>, <Datafile(path="another_file.txt")>])>

The ordering can also be carried out in reverse (i.e. descending order) by passing ``reverse=True`` as a second argument
to the ``order_by`` method.


--------------
``FilterDict``
--------------
The keys of a ``FilterDict`` can be anything, but each value must be a ``Filterable``. Hence, a ``FilterDict`` is
filtered and ordered by its values' attributes; when ordering, its items (key-value tuples) are returned in a
``FilterList``.

-----------------------
Using for your own data
-----------------------
If using filter containers for your own data, all the members must inherit from ``octue.mixins.filterable.Filterable``
to be filterable and orderable.
