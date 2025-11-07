# Creating services

One of the main features of Twined is to allow you to easily
create services that can accept questions and return answers. They can
run locally on any machine or be deployed to the cloud. Currently:

- The backend communication between twins uses Google Pub/Sub whether
  they're local or deployed
- Services are deployed to a Kubernetes/Kueue cluster
- The language of the entrypoint must by `python3` (you can call
  processes using other languages within this though)

## Anatomy of a Twined service

A Twined service is defined by the following files (located in the repository root by default).

### app.py

This is the entrypoint into your code - read more [here](creating_apps.md).

### twine.json

This file defines the schema for the service's configuration, input, and output data. Read more [here](twines.md) and
see an example [here](twine_file_quickstart.md).

### Dependencies file

A file specifying your app's dependencies. This is a [setup.py
file](https://docs.python.org/3/distutils/setupscript.html), a
[requirements.txt
file](https://learnpython.com/blog/python-requirements-file/), or a
[pyproject.toml file](https://python-poetry.org/docs/pyproject/) listing
all the python packages your app depends on and the version ranges that
are supported.

### octue.yaml

??? example "This describes the service configuration - read more..."

    This file defines the basic structure of your service. It must contain
    at least:

    ``` yaml
    services:
     - namespace: my-organisation
       name: my-app
    ```
    It may also need the following key-value pairs:

    - `app_source_path: <path>` - if your `app.py` file is not in the repository root

    All paths should be relative to the repository root. Other valid
    entries can be found in the `ServiceConfiguration` constructor.

    !!! warning

        Currently, only one service can be defined per repository, but it must
        still appear as a list item of the "services" key. At some point, it
        will be possible to define multiple services in one repository.

    If a service's app needs any configuration, asks questions to any
    other Twined services, or produces output datafiles/datasets, you will
    need to provide some or all of the following values for that service:

    - `configuration_values`
    - `configuration_manifest`
    - `children`
    - `output_location`
    - `use_signed_urls_for_output_datasets`

### Dockerfile (optional)

??? example "Provide this if your needs exceed the default Octue Dockerfile - read more..."

    Twined services run in a Docker container if they are deployed. They
    can also run this way locally. The SDK provides a default `Dockerfile`
    for these purposes that will work for most cases:

    - For deploying to [Kubernetes](https://github.com/octue/octue-sdk-python/blob/main/octue/twined/cloud/deployment/dockerfiles/Dockerfile-python313)

    However, you may need to write and provide your own `Dockerfile` if
    your app requires:

    - Non-python or system dependencies (e.g. `openfast`, `wget`)
    - Python dependencies that aren't installable via `pip`
    - Private python packages

    Here are two examples of a custom `Dockerfile` that use different base
    images:

    - [A TurbSim service](https://github.com/octue/turbsim-service/blob/main/Dockerfile)
    - [An OpenFAST service](https://github.com/octue/openfast-service/blob/main/Dockerfile)

    If you do provide one, you must provide its path relative to your
    repository to the `build-twined-services` GitHub Actions [workflow](https://github.com/octue/workflows/blob/main/.github/workflows/build-twined-service.yml).

    As always, if you need help with this, feel free to drop us a message or raise an issue!

### Where to specify the namespace, name, and revision tag

See [here](services.md/#service-names) for service naming requirements.

**Namespace**

- Required: yes
- Set in:
  - `octue.yaml`
  - `OCTUE_SERVICE_NAMESPACE` environment variable (takes priority)

**Name**

- Required: yes
- Set in:
  - `octue.yaml`
  - `OCTUE_SERVICE_NAME` environment variable (takes priority)

**Revision tag**

- Required: no
- Default: a random "coolname" (e.g. `hungry-hippo`)
- Set in:
  - `OCTUE_SERVICE_REVISION_TAG` environment variable
  - If using `octue twined service start` command, the `--revision-tag` option (takes priority)

## Template apps

We've created some template apps for you to look at and play around
with. We recommend going through them in this order:

1.  The [fractal app template](https://github.com/octue/octue-sdk-python/tree/main/octue/twined/templates/template-fractal) - introduces a basic Twined service that returns output values to its
    parent.
2.  The [using-manifests app template](https://github.com/octue/octue-sdk-python/tree/main/octue/twined/templates/template-using-manifests) - introduces using a manifest of output datasets to return output
    files to its parent.
3.  The [child-services app template](https://github.com/octue/octue-sdk-python/tree/main/octue/twined/templates/template-child-services) - introduces asking questions to child services and using their
    answers to form an output to return to its parent.

## Deploying services automatically

Automated deployment with Octue means:

- Your service runs in Google Kubernetes Engine (GKE), ready to accept
  questions from and return answers to other services.
- You don't need to do anything to update your deployed service with
  new code changes - the service simply gets rebuilt and re-deployed
  each time you push a commit to your `main` branch, or merge a pull
  request into it (other branches and deployment strategies are
  available, but this is the default).
- Serverless is the default - your service only runs when questions from
  other services are sent to it, meaning there are minimal costs to
  having it deployed but not in use.

If you'd like help deploying services, [contact us](../support.md). To do it yourself, see [here](deploying_services.md).
