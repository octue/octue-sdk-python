# Manifests

!!! tip

    Use a manifest to send [datasets](../datasets) to a Twined service as a question (for processing) - the
    service will send an output manifest back with its answer if the answer includes output datasets.

## Key features

### Group related datasets together

Make a clear grouping of datasets needed for a particular analysis.

```python
from octue.resources import Manifest

manifest = Manifest(
    datasets={
        "my_dataset_0": "gs://my-bucket/my_dataset_0",
        "my_dataset_1": "gs://my-bucket/my_dataset_1",
        "my_dataset_2": "gs://another-bucket/my_dataset_2",
    }
)
```

### Send datasets to a service

Get a Twined service to analyse data for you as part of a larger
analysis.

```python
from octue.twined.resources import Child

child = Child(
    id="octue/wind-speed:2.1.0",
    backend={"name": "GCPPubSubBackend", "project_id": "my-project"},
)

answer, question_uuid = child.ask(input_manifest=manifest)
```

See [here](../../asking_questions) for more information.

### Receive datasets from a service

Access output datasets from a Twined service from the cloud when you're
ready.

```python
manifest = answer["output_manifest"]
manifest["an_output_dataset"].files
>>> <FilterSet({<Datafile('my_file.csv')>, <Datafile('another_file.csv')>})>
```

!!! hint

    Datasets in an output manifest are stored in the cloud. You'll need to
    keep a reference to where they are to access them - the output manifest
    is this reference. You'll need to use it straight away or save it to
    make use of it.

### Download all datasets from a manifest

Download all or a subset of datasets from a manifest.

```python
manifest.download()
>>> {
    "my_dataset": "/path/to/dataset"
}
```

!!! note

    Datasets are downloaded to a temporary directory if no paths are given.

## Further information

### Manifests of local datasets

You can include local datasets in your manifest if you can guarantee all
services that need them can access them. A use case for this is, for
example, a supercomputer cluster running several Twined services
locally that process and transfer large amounts of data. It is much
faster to store and access the required datasets locally than upload
them to the cloud and then download them again for each service (as
would happen with cloud datasets).

!!! warning

    If you want to ask a child a question that includes a manifest containing one or more local datasets, you must
    include the `allow_local_files` parameter. For example, if you have an analysis object with a child called "wind_speed":

    ``` python
    input_manifest = Manifest(
        datasets={
            "my_dataset_0": "gs://my-bucket/my_dataset_0",
            "my_dataset_1": "local/path/to/my_dataset_1",
        }
    )

    answer, question_uuid = analysis.children["wind_speed"].ask(
        input_values=analysis.input_values,
        input_manifest=analysis.input_manifest,
        allow_local_files=True,
    )
    ```
