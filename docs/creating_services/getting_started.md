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
- Have access to an existing Twined services network - see [authentication instructions](/using_services/authentication)
  and [managing infrastructure](../../managing_infrastructure/getting_started)

<!-- prettier-ignore-end -->

## Create and clone a GitHub repository

!!! warning

    The repository must be created in the same GitHub account used for your Twined services network. See
    [this issue](https://github.com/octue/octue-sdk-python/issues/740) for more details.

Create a GitHub repository for the service. Clone this repository to your computer and checkout a new branch called
`add-new-service`. Replacing `<handle>` with your GitHub account handle.

```shell
git clone https://github.com/<handle>/example-service.git
cd example-service
git checkout -b add-new-service
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

Now install the dependencies:

```shell
poetry install
```

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

## Add the Twine file

The Twine file is a JSON file containing schemas stating what kind of data is expected as inputs and outputs of the
service. It shows users what can be sent to the service and what to expect to receive. Inputs and outputs that violate
the schemas won't be processed and will cause an error.

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

## Add the `octue.yaml` file

The service configuration file (called `octue.yaml`) names the service and sets details like where to store output data.
Create an `octue.yaml` file at the top level of the repository, replacing:

- `<handle>` with your GitHub account handle
- `<gcp-project-id>` with the ID of the Google Cloud Platform (GCP) project the Twined service network is deployed in
- `<region>` with the name of the GCP region the service network is deployed in (e.g. `europe-west1`)
- `<environment>` with the name of the environment the service network is deployed in (`main` by default)

!!! tip

    Ask the manager of your Twined service network to help you with these values!

```yaml
services:
  - namespace: <handle>
    name: example-service
    app_source_path: example_service
    event_store_table_id: octue_twined.service-events
    diagnostics_cloud_path: gs://<gcp-project-id>-octue-twined/example-service/diagnostics
    output_location: gs://<gcp-project-id>-octue-twined/example-service/outputs
    service_registries:
      - name: <handle>'s services
        endpoint: https://<region>-<gcp-project-id>.cloudfunctions.net/<environment>-octue-twined-service-registry
```

## Enable GitHub Actions in the repository

Go back to your repository on GitHub and open [its Actions settings](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository#managing-github-actions-permissions-for-your-repository)
(Settings -> Actions -> General). Set the "Actions permissions" option to "Allow all actions and reusable workflows".

## Add the GitHub Actions release workflow

A GitHub Actions reusable workflow is used to automatically deploy the service when its code is merged into `main`.
Create a file called `.github/workflows/release.yml` and add the following, replacing `<handle>` and
`<gcp-project-id>` as before:

```yaml
name: deploy

# Only trigger when a pull request into main branch is merged.
on:
  push:
    branches:
      - main

jobs:
  deploy:
    uses: octue/workflows/.github/workflows/build-twined-service.yml@0.11.0
    permissions:
      id-token: write
      contents: read
    with:
      gcp_project_name: <gcp-project-id>
      gcp_project_number: <gcp-project-number>
      gcp_region: <gcp-project-region>
      service_namespace: <handle>
      service_name: example-service
```

!!! tip

    See [here](https://github.com/octue/workflows?tab=readme-ov-file#deploying-a-kuberneteskueue-octue-twined-service-revision)
    for more information, including how to use custom dockerfiles for your service.

## Merge the code into `main`

To deploy the service, we need to merge the code we've added into the `main` branch. Make sure any sensitive and
irrelevant files are listed in a `.gitignore` file and run:

```shell
git add .
git commit -m "Add example Twined service"
git push
```

For best practice, open a pull request for your branch into `main`, review it, and merge it. For a simpler route:

```shell
git checkout main
git merge add-new-service
git push
```

Navigate to your repository's page on GitHub and you should see the release workflow progressing after a few seconds. An
in-progress indicator (currently a small orange circle) will be shown against the most recent commit.

## Ask the service its first question

Once the release workflow has completed (which should take only a couple of minutes for this simple example service), a
green tick should show next to the most recent commit. You can now communicate with the service over the internet to ask
it a question! From the terminal where you ran `poetry install`, replace `<handle>` as before and run:

```shell
octue twined question ask <handle>/example-service:0.1.0 --input-values='{"some_input": 1}'
```

After a couple of minutes (while the Kubernetes cluster is spinning up a container to run the service), you should see
log messages start to appear and finally see `[1, 2, 3, 4, 5]` returned as output values.

See the [using services getting started guide](/using_services/getting_started) to see how to ask questions in python
instead.

## Next steps

- [Read about the core concepts of Twined](/core_concepts)
- [Create infrastructure for a Twined service network](/managing_infrastructure/getting_started)
