Turn your analysis code into production-ready services - no infrastructure or DevOps skills needed - and share with
your team and beyond.

This guide walks you through creating an example Twined service deployed in the cloud. By the end, you'll have a real
service and be able to ask it questions from your computer and receive answers from it over the internet.

## Prerequisites

Before you begin, ensure you:

<!-- prettier-ignore-start -->

- Are familiar with Python and the command line
- Have the following tools installed:
    - Python >= 3.10
    - The `octue` python library / CLI (see [installation instructions](../installation.md))
- Have access to an existing Twined services network - see [authentication instructions](/creating_services/authentication)
- Have the ability to create a GitHub repository under the same GitHub account or organisation as used for the
  [`github_account` variable](https://github.com/octue/terraform-octue-twined-core?tab=readme-ov-file#input-reference)
  used for the Twined services network

<!-- prettier-ignore-end -->

## Create and clone a GitHub repository

Create a GitHub repository for the service. It must be owned by the GitHub account used as the `github_account` variable
for the Twined services network. This variable is an input to the
[Twined core Terraform module](https://github.com/octue/terraform-octue-twined-core?tab=readme-ov-file#input-reference)
and should be stated in a `variables.tf` or another `.tf` file in the Terraform configuration used to deploy the services
network. Ask the person who created or manages your infrastructure if you're not sure.

Clone this repository to your computer

```shell
git clone <my-repository>
```

## Install the python dependencies

Create a `pyproject.toml` file to define the service as a python package and list its dependencies:

```toml
[tool.poetry]
name = "example-service"
version = "0.1.0"
description = "An example Twined data service."
authors = ["Your name <your email>"]
packages = [{include = "example_service"}]

[tool.poetry.dependencies]
python = "^3.11"
octue = "0.69.0"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

!!! tip

    We use Poetry in this example but you can use Pip or another package manager with either a `setup.py` or
    `pyproject.toml` file.

## Write the service python code

We'll make the example service do the most basic thing possible - return dummy output values and a dummy output dataset.

Create a directory called `example_service` with an empty `__init__.py` file and an `app.py` file inside. In `app.py`,
add the following code:

```python
import logging
import os
import tempfile
import time

from octue.resources import Datafile, Dataset

logger = logging.getLogger(__name__)


def run(analysis):
    logger.info("Started example analysis.")
    time.sleep(2)
    analysis.output_values = [1, 2, 3, 4, 5]

    with tempfile.TemporaryDirectory() as temporary_directory:
        with Datafile(os.path.join(temporary_directory, "output.dat"), mode="w") as (datafile, f):
            f.write("This is some example service output.")

        analysis.output_manifest.datasets["example_dataset"] = Dataset(path=temporary_directory, files={datafile})

    logger.info("Finished example analysis.")
```

## Add the twine file

The Twine file is a JSON file stating what kind of data is expected as inputs and outputs of the service. It shows users
what can be sent to the service and what to expect as output. Inputs and outputs that violate the schemas won't be
processed and will cause an error.

Create a file at the top level of the repository called `twine.json`:

```json
{
  "input_values_schema": {
    "type": "object",
    "required": ["some_input"],
    "properties": {
      "some_input": {
        "type": "integer"
      }
    }
  },
  "output_values_schema": {
    "title": "Output values",
    "description": "Some dummy output data.",
    "type": "array",
    "items": {
      "type": "number"
    }
  },
  "output_manifest": {
    "datasets": {
      "example_dataset": {}
    }
  }
}
```

## Add the service configuration file

The service configuration file names the service and tells it things like where to store output data. Create an
`octue.yaml` file, replacing `<handle>` with the value of `github_account` mentioned above. Check the other values with
whoever manages your Twined service network (they can find them in the outputs of the Terraform modules used to deploy
the service network).

```yaml
services:
  - namespace: <handle>
    name: example-service
    app_source_path: example_service

    # Get these from whoever manages your Twined service network.
    event_store_table_id: octue_twined.service-events
    diagnostics_cloud_path: gs://<GCP project name>-octue-twined/example-service/diagnostics
    output_location: "gs://<GCP project name>-octue-twined/example-service/outputs"
    service_registries:
      - name: <handle>'s services
        endpoint: https://europe-west9-octue-twined-services.cloudfunctions.net/<environment>-octue-twined-service-registry
```

## Add the GitHub Actions reusable workflow

## Send the service its first question

## Next steps
