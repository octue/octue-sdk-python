.. _data_containers:

==================================
Datafiles, datasets, and manifests
==================================

One of the main features of ``octue`` is to make data management easy. There are three main data classes in the SDK to
make this possible.

``Datafile``
============
A single local or cloud file.

Key features:

- Working with a datafile is the same whether it's local or in the cloud. For example, this is how to write to local and
  cloud datafiles:

  .. code-block::

      # Local datafile
      datafile = Datafile("path/to/file.dat")
      with datafile.open("w") as f:
          f.write("Some data")
          datafile.labels.add("processed")

      # Cloud datafile
      datafile = Datafile("gs://my-bucket/path/to/file.dat")
      with datafile.open("w") as f:
          f.write("Some data")
          datafile.labels.add("processed")

- If you're reading or writing a cloud datafile, its content is automatically downloaded so you get low-latency data
  operations

- Luckily, downloading is lazy - until you access the cloud datafile contents using the ``open`` context manager, call
  the ``download`` method, or try to get its ``local_path`` attribute, the content isn't downloaded. This makes viewing
  and filtering by the metadata of datasets of cloud datafiles quick.

- Need to run a CLI command on your file as part of your app?

  .. code-block::

      output = subprocess.check_output(["cli-app", "datafile.local_path"])

- Metadata is stored locally for local datafiles and in the cloud for cloud datafiles:
  - ID
  - Timestamp
  - Arbitrary labels (strings)
  - Arbitrary tags (key-value pairs)

  These are read from the cloud object or a local ``.octue`` file on ``Datafile`` instantiation and can be accessed simply:

  .. code-block::

      datafile.id
      >>> '9a1f9b26-6a48-4f2d-be80-468d3270d79b'

      datafile.timestamp
      >>> datetime.datetime(2022, 5, 4, 17, 57, 57, 136739)

      datafile.labels
      >>> {"processed"}

      datafile.tags
      >>> {"organisation": "octue", "energy": "renewable"}


  Metadata can be set on the instance by setting the attributes but the cloud/local metadata stores aren't updated by
  default. You can update them by doing:

  .. code-block::

      datafile.update_local_metadata()

      datafile.update_cloud_metadata()

- You can upload a local dataset to the cloud without using the ``open`` context manager:

  .. code-block::

      local_datafile.to_cloud("gs://my-bucket/my_datafile.dat", update_cloud_metadata=True)

``Dataset``
===========
A grouping of any number of datafiles that exist in the same place, typically produced by or for one operation in an analysis.

``Manifest``
============
A grouping of any number of datasets, typically produced by or for one analysis
