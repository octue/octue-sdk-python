# Manifest strands

Frequently, twins operate on files containing some kind of data. These
files need to be made accessible to the code running in the twin, in
order that their contents can be read and processed. Conversely, a twin
might produce an output dataset which must be understood by users.

The `configuration_manifest`, `input_manifest` and `output_manifest`
strands describe what kind of datasets (and associated files) are
required / produced.

!!! note

    Files are always contained in datasets, even if there's only one file.
    It's so that we can keep nitty-gritty file metadata separate from the
    more meaningful, higher level metadata like what a dataset is for.

## Configuration manifest strand

This describes datasets/files that are required at startup of the twin /
service. They typically contain a resource that the twin might use
across many analyses.

For example, a twin might predict failure for a particular component,
given an image. It will require a trained ML model (saved in a
`*.pickle` or `*.json`). While many thousands of predictions might be
done over the period that the twin is deployed, all predictions are done
using this version of the model - so the model file is supplied at
startup.

### Example

Here's a twine containing this strand:

```json
{
  // Manifest strands contain lists, with one entry for each required dataset
  "configuration_manifest": {
    "datasets": [
      {
        // Once the inputs are validated, your analysis program can use this key to access the dataset
        "key": "trained_model",
        // General notes, which are helpful as a reminder to users of the service
        "purpose": "The trained classifier"
      }
    ]
  }
}
```

Here's a manifest that's valid for the twine:

```json
{
  "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
  "datasets": [
    {
      "id": "7ead7669-8162-4f64-8cd5-4abe92509e17",
      "name": "training data for system abc123",
      "organisation": "megacorp",
      "tags": { "system": "abc123" },
      "labels": ["classifier", "damage"],
      "files": [
        {
          "path": "datasets/7ead7669/blade_damage.mdl",
          "cluster": 0,
          "sequence": 0,
          "extension": "csv",
          "tags": {},
          "labels": [],
          "posix_timestamp": 0,
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
          "last_modified": "2019-02-28T22:40:30.533005Z",
          "name": "blade_damage.mdl",
          "size_bytes": 59684813,
          "sha-512/256": "somesha"
        }
      ]
    }
  ]
}
```

## Input manifest strand

These files are made available for the twin to run a particular analysis
with. Each analysis will likely have different input datasets.

For example, a twin might be passed a dataset of LiDAR `*.scn` files and
be expected to compute atmospheric flow properties as a timeseries
(which might be returned in the [output values](values.md#output-values-strand) for
onward processing and storage).

### Example

Here we specify that two datasets (and all or some of the files
associated with them) are required, for a service that cross-checks
meteorological mast data and power output data for a wind farm.

Here's a twine containing this strand:

```json
{
  // Manifest strands contain lists, with one entry for each required dataset
  "input_manifest": {
    "datasets": [
      {
        // Once the inputs are validated, your analysis program can use this key to access the dataset
        "key": "met_mast_data",
        // General notes, which are helpful as a reminder to users of the service
        "purpose": "A dataset containing meteorological mast data"
      },
      {
        "key": "scada_data",
        "purpose": "A dataset containing scada data"
      }
    ]
  }
}
```

Here's a manifest valid for the twine:

```json
{
  "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
  "datasets": [
    {
      "id": "7ead7669-8162-4f64-8cd5-4abe92509e17",
      "name": "meteorological mast dataset",
      "tags": { "location": 108346 },
      "labels": ["met", "mast", "wind"],
      "files": [
        {
          "path": "input/datasets/7ead7669/mast_1.csv",
          "cluster": 0,
          "sequence": 0,
          "extension": "csv",
          "tags": {},
          "labels": [],
          "posix_timestamp": 1551393630,
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
          "last_modified": "2019-02-28T22:40:30.533005Z",
          "name": "mast_1.csv",
          "size_bytes": 59684813,
          "sha-512/256": "somesha"
        },
        {
          "path": "input/datasets/7ead7669/mast_2.csv",
          "cluster": 0,
          "sequence": 1,
          "extension": "csv",
          "tags": {},
          "labels": [],
          "posix_timestamp": 1551394230,
          "id": "bbff07bc-7c19-4ed5-be6d-a6546eae8e45",
          "last_modified": "2019-02-28T22:50:40.633001Z",
          "name": "mast_2.csv",
          "size_bytes": 59684813,
          "sha-512/256": "someothersha"
        }
      ]
    },
    {
      "id": "5cf9e445-c288-4567-9072-edc31003b022",
      "name": "scada data exports",
      "tags": { "location": 108346, "system": "ab32" },
      "labels": ["wind", "turbine", "scada"],
      "files": [
        {
          "path": "input/datasets/7ead7669/export_1.csv",
          "cluster": 0,
          "sequence": 0,
          "extension": "csv",
          "tags": {},
          "labels": [],
          "posix_timestamp": 1551393600,
          "id": "78fa511f-3e28-4bc2-aa28-7b6a2e8e6ef9",
          "last_modified": "2019-02-28T22:40:00.000000Z",
          "name": "export_1.csv",
          "size_bytes": 88684813,
          "sha-512/256": "somesha"
        },
        {
          "path": "input/datasets/7ead7669/export_2.csv",
          "cluster": 0,
          "sequence": 1,
          "extension": "csv",
          "tags": {},
          "labels": [],
          "posix_timestamp": 1551394200,
          "id": "204d7316-7ae6-45e3-8f90-443225b21226",
          "last_modified": "2019-02-28T22:50:00.000000Z",
          "name": "export_2.csv",
          "size_bytes": 88684813,
          "sha-512/256": "someothersha"
        }
      ]
    }
  ]
}
```

## Output manifest strand

Files are created by the twin during an analysis, tagged and stored as
datasets for some onward purpose. This strand is not used for sourcing
data; it enables users or other services to understand appropriate
search terms to retrieve datasets produced.

### Example

Here's a twine containing this strand:

```json
{
  "output_manifest": {
    "datasets": [
      {
        // Twined will prepare a manifest with this key, which you can add to during the analysis or once its complete
        "key": "met_scada_checks",
        // General notes, which are helpful as a reminder to users of the service
        "purpose": "A dataset containing figures showing correlations between mast and scada data"
      }
    ]
  }
}
```

Here's a manifest valid for the twine:

```json
{
  "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
  "datasets": [
    {
      "id": "4564deca-5654-42e8-aadf-70690b393a30",
      "name": "visual cross check data",
      "organisation": "megacorp",
      "tags": { "location": 108346 },
      "labels": ["figure", "met", "mast", "scada", "check"],
      "files": [
        {
          "path": "datasets/7ead7669/cross_check.fig",
          "cluster": 0,
          "sequence": 0,
          "extension": "fig",
          "tags": {},
          "labels": [],
          "posix_timestamp": 1551394800,
          "id": "38f77fe2-c8c0-49d1-a08c-0928d53a742f",
          "last_modified": "2019-02-28T23:00:00.000000Z",
          "name": "cross_check.fig",
          "size_bytes": 59684813,
          "sha-512/256": "somesha"
        }
      ]
    }
  ]
}
```

## File tag templates

Datafiles can be tagged with key-value pairs of relevant metadata that
can be used in analyses. Certain datasets might need one set of metadata
on each file, while others might need a different set. The required (or
optional) file tags can be specified in the twine in the
`file_tags_template` property of each dataset of any `manifest` strand.
Each file in the corresponding manifest strand is then validated against
its dataset's file tag template to ensure the required tags are
present.

Here's a manifest strand with a file tag template. It's for an input manifest, but the format is the
same for configuration and output manifests.

```json
{
  "input_manifest": {
    "datasets": [
      {
        "key": "met_mast_data",
        "purpose": "A dataset containing meteorological mast data",
        "file_tags_template": {
          "type": "object",
          "properties": {
            "manufacturer": { "type": "string" },
            "height": { "type": "number" },
            "is_recycled": { "type": "boolean" }
          },
          "required": ["manufacturer", "height", "is_recycled"]
        }
      }
    ]
  }
}
```

Here's a manifest valid for the twine:

```json
{
  "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
  "datasets": [
    {
      "id": "7ead7669-8162-4f64-8cd5-4abe92509e17",
      "name": "met_mast_data",
      "tags": {},
      "labels": ["met", "mast", "wind"],
      "files": [
        {
          "path": "input/datasets/7ead7669/file_1.csv",
          "cluster": 0,
          "sequence": 0,
          "extension": "csv",
          "labels": ["mykeyword1", "mykeyword2"],
          "tags": {
            "manufacturer": "vestas",
            "height": 500,
            "is_recycled": true
          },
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
          "name": "file_1.csv"
        },
        {
          "path": "input/datasets/7ead7669/file_1.csv",
          "cluster": 0,
          "sequence": 1,
          "extension": "csv",
          "labels": [],
          "tags": {
            "manufacturer": "vestas",
            "height": 500,
            "is_recycled": true
          },
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
          "name": "file_1.csv"
        }
      ]
    }
  ]
}
```

A remote reference can also be given for a file tag template. If the tag
template somewhere public, this is useful for sharing the template
between one or more teams working on the same type of data.

The example below is for an input manifest, but the format is the same
for configuration and output manifests. It also shows two different tag
templates being specified for two different types of dataset required by
the manifest.

Here's a twine using a remote tag template:

```json
{
  "input_manifest": {
    "datasets": [
      {
        "key": "met_mast_data",
        "purpose": "A dataset containing meteorological mast data",
        "file_tags_template": {
          "$ref": "https://refs.schema.octue.com/octue/my-file-type-tag-template/0.0.0.json"
        }
      },
      {
        "key": "some_other_kind_of_dataset",
        "purpose": "A dataset containing something else",
        "file_tags_template": {
          "$ref": "https://refs.schema.octue.com/octue/another-file-type-tag-template/0.0.0.json"
        }
      }
    ]
  }
}
```

Here's a manifest valid for the twine:

```json
{
  "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
  "datasets": [
    {
      "id": "7ead7669-8162-4f64-8cd5-4abe92509e17",
      "name": "met_mast_data",
      "tags": {},
      "labels": ["met", "mast", "wind"],
      "files": [
        {
          "path": "input/datasets/7ead7669/file_1.csv",
          "cluster": 0,
          "sequence": 0,
          "extension": "csv",
          "labels": ["mykeyword1", "mykeyword2"],
          "tags": {
            "manufacturer": "vestas",
            "height": 500,
            "is_recycled": true
          },
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
          "name": "file_1.csv"
        },
        {
          "path": "input/datasets/7ead7669/file_1.csv",
          "cluster": 0,
          "sequence": 1,
          "extension": "csv",
          "labels": [],
          "tags": {
            "manufacturer": "vestas",
            "height": 500,
            "is_recycled": true
          },
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
          "name": "file_1.csv"
        }
      ]
    },
    {
      "id": "7ead7669-8162-4f64-8cd5-4abe92509e29",
      "name": "some_other_kind_of_dataset",
      "tags": {},
      "labels": ["my-label"],
      "files": [
        {
          "path": "input/datasets/7eadpp9/interesting_file.dat",
          "cluster": 0,
          "sequence": 0,
          "extension": "dat",
          "labels": [],
          "tags": {
            "length": 864,
            "orientation_angle": 85
          },
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae9071",
          "name": "interesting_file.csv"
        }
    }
  ]
}
```
