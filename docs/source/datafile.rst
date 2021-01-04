.. _datafile:

========
Datafile
========

A ``Datafile`` is an Octue type that corresponds to any file on your computer. It has the following main attributes:

- ``path`` - the path of this file, which may include folders or subfolders, within the dataset.
- ``cluster`` - the integer cluster of files, within a dataset, to which this belongs (default 0)
- ``sequence`` - a sequence number of this file within its cluster (if sequences are appropriate)
- ``tags`` - a space-separated string or iterable of tags relevant to this file
- ``posix_timestamp`` - a posix timestamp associated with the file, in seconds since epoch, typically when it was created but could relate to a relevant time point for the data
