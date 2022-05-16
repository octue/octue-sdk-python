[![PyPI version](https://badge.fury.io/py/octue.svg)](https://badge.fury.io/py/octue)
[![Release](https://github.com/octue/octue-sdk-python/actions/workflows/release.yml/badge.svg)](https://github.com/octue/octue-sdk-python/actions/workflows/release.yml)
[![codecov](https://codecov.io/gh/octue/octue-sdk-python/branch/main/graph/badge.svg?token=4KdR7fmwcT)](https://codecov.io/gh/octue/octue-sdk-python)
[![Documentation Status](https://readthedocs.org/projects/octue-python-sdk/badge/?version=latest)](https://octue-python-sdk.readthedocs.io/en/latest/?badge=latest)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

# octue-sdk-python <span><img src="http://slurmed.com/fanart/javier/213_purple-fruit-snake.gif" alt="Purple Fruit Snake" width="100"/></span>

Utilities for running python based data services, digital twins and applications. [Documentation is here!](https://octue-python-sdk.readthedocs.io/en/latest/)

Based on the [twined](https://twined.readthedocs.io/en/latest/) library for data validation.

## Installation and usage
For usage as a scientist or engineer, run one of the following commands in your environment:
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

### Deprecated code
When code is deprecated, it will still work but a deprecation warning will be issued with a suggestion on how to update
it. After an adjustment period, deprecations will be removed from the codebase according to the [code removal schedule](https://github.com/octue/octue-sdk-python/issues/415).
This constitutes a breaking change.

## Developer notes

### Installation
We use [Poetry](https://python-poetry.org/) as our package manager. For development, run the following from the
repository root, which will editably install the package:

```bash
poetry install -E dataflow -E hdf5
```

Then run the tests to check everything's working.

### Testing
These environment variables need to be set to run the tests:
* `GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service/account/file.json`
* `TEST_PROJECT_NAME=<name-of-google-cloud-project-to-run-pub-sub-tests-on>`

Then, from the repository root, run
```bash
python3 -m unittest
```

**Documentation for use of the library is [here](https://octue-python-sdk.readthedocs.io). You don't need to pay attention to the following unless you plan to develop `octue-sdk-python` itself.**


## Contributing
Take a look at our [contributing](/docs/contributing.md) page.
