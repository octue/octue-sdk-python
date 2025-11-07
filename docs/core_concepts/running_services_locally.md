Services can be operated locally (e.g. for testing or ad-hoc data
processing). You can:

- Run your service once (i.e. run one analysis):
  - Via the `octue twined` CLI
  - By using the `octue` library in a python script
- Start your service as a child, allowing it to answer any number of
  questions from any other Twined service:
  - Via the CLI

## Running a service once

### Via the CLI

1.  Ensure you've created a valid
    [`octue.yaml`](../core_concepts/creating_services.md/#octueyaml)
    file for your service

2.  Run:

    > ```shell
    > octue twined question ask local --input-values='{"some": "input"}'
    > ```

The output values and/or manifest will be printed to `stdout` but are
also stored in the event store.

### Via a python script

Imagine we have a simple app that calculates the area of a square. It
could be run locally on a given height and width like this:

```python
from octue.twined.runner import Runner

runner = Runner(app_src="path/to/app.py", twine="path/to/twine.json")
analysis = runner.run(input_values={"height": 5, "width": 10})

analysis.output_values
>>> {"area": 50}

analysis.output_manifest
>>> None
```

See the `Runner` API documentation for more advanced usage including providing configuration, children, and an input manifest.

## Starting a service as a child

### Via the CLI

1.  Ensure you've created a valid
    [`octue.yaml`](../core_concepts/creating_services.md/#octueyaml)
    file for your service

2.  Run:

    > ```shell
    > octue twined service start
    > ```

This will run the service as a child waiting for questions until you
press `Ctrl + C` or an error is encountered. The service will be
available to be questioned by other services at the service ID
`organisation/name` as specified in the `octue.yaml` file.

!!! tip

    You can use the `--timeout` option to stop the service after a given
    number of seconds.
