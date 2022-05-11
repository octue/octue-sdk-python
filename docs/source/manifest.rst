.. _manifest:

========
Manifest
========
A set of related cloud and/or local :doc:`datasets <dataset>` plus metadata. Typically produced by or needed for an
analysis.

Key features
============

Group related datasets together
-------------------------------

.. code-block:: python

    from octue.resources import Manifest

    manifest = Manifest(
        datasets={
            "my_dataset_0": "gs://my-bucket/my_dataset_0",
            "my_dataset_1": "gs://my-bucket/my_dataset_1",
            "my_dataset_2": "gs://another-bucket/my_dataset_2",
        }
    )


Send datasets to a digital twin
-------------------------------

.. code-block::

    from octue.resources import Child

    child = Child(
        name="wind_speed",
        id="4acbf2aa-54ce-4bae-b473-a062e21b3d57",
        backend={"name": "GCPPubSubBackend", "project_name": "my-project"},
    )

    answer = child.ask(input_manifest=manifest)


Store manifests locally
-----------------------

.. code-block:: python

    with open("manifest.json", "w") as f:
        json.dump(manifest.to_primitive(), f)

    with open("manifest.json") as f:
        reloaded_manifest = Manifest.deserialise(json.load(f))


Store manifests in the cloud
----------------------------

.. code-block:: python

    manifest.to_cloud("gs://my-bucket/path/to/manifest.json")

    reloaded_manifest = Manifest.from_cloud("gs://my-bucket/path/to/my_manifest.json")


Further information
===================

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
              datasets={
                  "my_dataset_0": "gs://my-bucket/my_dataset_0",
                  "my_dataset_1": "local/path/to/my_dataset_1",
              }
          )

          analysis.children["wind_speed"].ask(
              input_values=analysis.input_values,
              input_manifest=analysis.input_manifest,
              allow_local_files=True,
          )
