[![PyPI version](https://badge.fury.io/py/octue.svg)](https://badge.fury.io/py/octue)
[![Release](https://github.com/octue/octue-sdk-python/actions/workflows/release.yml/badge.svg)](https://github.com/octue/octue-sdk-python/actions/workflows/release.yml)
[![codecov](https://codecov.io/gh/octue/octue-sdk-python/branch/main/graph/badge.svg?token=4KdR7fmwcT)](https://codecov.io/gh/octue/octue-sdk-python)
[![Documentation Status](https://readthedocs.org/projects/octue-python-sdk/badge/?version=latest)](https://octue-python-sdk.readthedocs.io/en/latest/?badge=latest)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

# Octue Python SDK <img src="./docs/source/images/213_purple-fruit-snake-transparent.gif" alt="Purple Fruit Snake" width="100"/></span>

The python SDK for running [Octue](https://octue.com) data services, digital twins, and applications - get faster data
groundwork so you have more time for the science!

Read the docs [here.](https://octue-python-sdk.readthedocs.io/en/latest/)

Uses our [twined](https://twined.readthedocs.io/en/latest/) library for data validation.

## Installation and usage
To install, run one of:
```shell
pip install octue
```

```shell
poetry add octue
```

The command line interface (CLI) can then be accessed via:
```shell
octue --help
```

```text
Usage: octue [OPTIONS] COMMAND [ARGS]...

  The CLI for the Octue SDK. Use it to start an Octue data service or digital
  twin locally or run an analysis on one locally.

  Read more in the docs: https://octue-python-sdk.readthedocs.io/en/latest/

Options:
  --id UUID                       UUID of the analysis being undertaken. None
                                  (for local use) will cause a unique ID to be
                                  generated.
  --logger-uri TEXT               Stream logs to a websocket at the given URI.
  --log-level [debug|info|warning|error]
                                  Log level used for the analysis.  [default:
                                  info]
  --force-reset / --no-force-reset
                                  Forces a reset of analysis cache and outputs
                                  [For future use, currently not implemented]
                                  [default: force-reset]
  --version                       Show the version and exit.
  -h, --help                      Show this message and exit.

Commands:
  deploy  Deploy a python app to the cloud as an Octue service or digital...
  run     Run an analysis on the given input data using an Octue service...
  start   Start an Octue service or digital twin locally as a child so it...
```

## Deprecated code
When code is deprecated, it will still work but a deprecation warning will be issued with a suggestion on how to update
it. After an adjustment period, deprecations will be removed from the codebase according to the [code removal schedule](https://github.com/octue/octue-sdk-python/issues/415).
This constitutes a breaking change.

## Developer notes

### Installation
We use [Poetry](https://python-poetry.org/) as our package manager. For development, run the following from the
repository root, which will editably install the package:

```shell
poetry install --all-extras
```

Then run the tests to check everything's working.

### Testing
These environment variables need to be set to run the tests:
* `GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service/account/file.json`
* `TEST_PROJECT_NAME=<name-of-google-cloud-project-to-run-pub-sub-tests-on>`

Then, from the repository root, run
```shell
python3 -m unittest
```
or
```shell
tox
```

## Contributing
Take a look at our [contributing](/docs/contributing.md) page.
