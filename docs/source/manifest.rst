.. _manifest:

========
Manifest
========
A set of related datasets that exist anywhere, plus metadata. Typically produced by or for one analysis.

A manifest is a group of cloud and/or local datasets needed for an analysis. Only using cloud datasets guarantees that
every service (with the correct permissions) that is sent the manifest will be able to access the datasets. There can be
speed and cost benefits from using local datasets when dealing with large amounts of data, but the manifest can only
include them if you can guarantee that all relevant services can access them.

You can instantiate a manifest like this:

.. code-block:: python

    manifest = Manifest(
        datasets=[
            "gs://my-bucket/my_dataset_0",
            "gs://my-bucket/my_dataset_1",
            "gs://another-bucket/my_dataset_2",
        ],
        keys={"my_dataset_0: 0, "my_dataset_1": 1, "my_dataset_2": 2},
    )


Manifests of local datasets
---------------------------
You can include local datasets in your manifest if you can guarantee all services that need them can access them. A use
case for this is, for example, a supercomputer cluster running several ``octue`` services locally that process and
transfer large amounts of data. It is much faster to store and access the required datasets locally than upload them to
the cloud and then download them again for each service (as would happen with cloud datasets).

.. warning::

     If you want to ask a child a question that includes a manifest containing one or more local datasets, you must
     include the ``allow_local_files`` parameter. For example, if you have an analysis object with a child called
     "wind_speed":

     .. code-block:: python

        input_manifest = Manifest(
            datasets=[
                "gs://my-bucket/my_dataset_0",
                "local/path/to/my_dataset_1",
            ],
            keys={"my_dataset_0: 0, "my_dataset_1": 1},
        )

        analysis.children["wind_speed"].ask(
            input_values=analysis.input_values,
            input_manifest=analysis.input_manifest,
            allow_local_files=True,
        )


Storing manifests in the cloud
------------------------------
You can store a manifest as a JSON file in the cloud and retrieve it later:

.. code-block:: python

    manifest.to_cloud(cloud_path="gs://my-bucket/path/to/my_manifest.json")

    downloaded_manifest = Manifest.from_cloud(cloud_path="gs://my-bucket/path/to/my_manifest.json")
