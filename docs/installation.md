# Installation

The Twined framework is provided through the [Octue SDK](https://github.com/octue/octue-sdk-python).

## Pip

```shell
pip install octue
```

## Poetry

Read more about Poetry [here](https://python-poetry.org).

```shell
poetry add octue
```

## Check installation

If the installation worked correctly, the `octue` CLI will be available:

```shell
octue --help
```

```text
Usage: octue [OPTIONS] COMMAND [ARGS]...

  The CLI for Octue SDKs and APIs, most notably Twined.

  Read more in the docs: https://twined.octue.com

Options:
  --log-level [debug|info|warning|error]
                                  Log level used for the analysis.  [default:
                                  info]
  --version                       Show the version and exit.
  -h, --help                      Show this message and exit.

Commands:
  twined  The Twined CLI.
```
