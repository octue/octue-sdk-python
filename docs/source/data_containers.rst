.. _data_containers:

==================================
Datafiles, datasets, and manifests
==================================

One of the main features of ``octue`` is to make managing scientific datasets easy. There are three main data classes
in the SDK to make this possible.

``Datafile``
============
:doc:`A single local or cloud file <datafile>` and its metadata.


``Dataset``
===========
:doc:`A set of related datafiles <dataset>` that exist in the same location, plus metadata. Typically produced by or
for one operation in an analysis.


``Manifest``
============
:doc:`A set of related datasets <manifest>` that exist anywhere, plus metadata. Typically produced by or for one
analysis.
