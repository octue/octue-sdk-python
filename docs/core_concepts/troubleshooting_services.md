## Diagnostics

Services save the following data to the cloud if they crash while
processing a question (the default), or when they finish processing a
question successfully if diagnostics are permanently turned on (not the
default):

- Input values
- Input manifest and datasets
- Child configuration values
- Child configuration manifest and datasets
- Inputs to and events received in response to each question the service
  asked its children (if it has any). These are stored in the order the
  questions were asked.

!!! important

    For this feature to be enabled, the child must have the
    `diagnostics_cloud_path` field in its service configuration
    ([`octue.yaml`](../creating_services/#octueyaml) file) set to a Google Cloud Storage path.

## Accessing diagnostics

If diagnostics are enabled, a service will upload the diagnostics and
send the upload path to the parent as a log message. A user with
credentials to access this path can use the `octue` CLI to retrieve the
diagnostics data:

```shell
octue twined question diagnostics <cloud-path>
```

More information on the command:

```
>>> octue twined question diagnostics -h

Usage: octue twined question diagnostics [OPTIONS] CLOUD_PATH

  Download diagnostics for an analysis from the given directory in
  Google Cloud Storage. The cloud path should end in the analysis ID.

  CLOUD_PATH: The path to the directory in Google Cloud Storage containing the
  diagnostics data.

Options:
  --local-path DIRECTORY  The path to a directory to store the directory of
                          diagnostics data in. Defaults to the current working
                          directory.
  --download-datasets     If provided, download any datasets from the
                          diagnostics and update their paths in their
                          manifests to the new local paths.
  -h, --help              Show this message and exit.
```

## Creating test fixtures from diagnostics {#test_fixtures_from_diagnostics}

You can create test fixtures directly from diagnostics, allowing you to
recreate the exact conditions that caused your service to fail.

```python
from unittest.mock import patch

from octue.twined.runner import Runner
from octue.twined.utils.testing import load_test_fixture_from_diagnostics


(
    configuration_values,
    configuration_manifest,
    input_values,
    input_manifest,
    child_emulators,
) = load_test_fixture_from_diagnostics(path="path/to/downloaded/diagnostics")

# You can explicitly specify your children here as shown or
# read the same information in from your service configuration file.
children = [
    {
        "key": "my_child",
        "id": "octue/my-child-service:2.1.0",
        "backend": {
            "name": "GCPPubSubBackend",
            "project_id": "my-project",
        }
    },
    {
        "key": "another_child",
        "id": "octue/another-child-service:2.1.0",
        "backend": {
            "name": "GCPPubSubBackend",
            "project_id": "my-project",
        }
    }
]

runner = Runner(
    app_src="path/to/directory_containing_app",
    twine="twine.json",
    children=children,
    configuration_values=configuration_values,
    configuration_manifest=configuration_manifest,
    service_id="your-org/your-service:2.1.0",
)

with patch("octue.twined.runner.Child", side_effect=child_emulators):
    analysis = runner.run(input_values=input_values, input_manifest=input_manifest)
```

## Disabling diagnostics

When asking a question to a child, parents can disable diagnostics
upload in the child on a question-by-question basis by setting
`save_diagnostics` to `"SAVE_DIAGNOSTICS_OFF"` in `Child.ask`. For example:

```python
from octue.twined.resources import Child


child = Child(
    id="my-organisation/my-service:2.1.0",
    backend={"name": "GCPPubSubBackend", "project_id": "my-project"},
)

answer, question_uuid = child.ask(
    input_values={"height": 32, "width": 3},
    save_diagnostics="SAVE_DIAGNOSTICS_OFF",
)
```
