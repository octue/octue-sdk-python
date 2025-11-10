# Getting started - using services

Run analyses instantly on ready-made services - no cloud setup, DevOps, or coding required. Run once or thousands of
times, and add them to automation pipelines.

This guide walks you through using an example Twined service locally. The process for using a real one (deployed locally
or in the cloud) is almost identical.

By the end, you will be able to use the Twined CLI to run an analysis on a data service, sending it input data and
receiving output data.

## Prerequisites

Before you begin, ensure you:

<!-- prettier-ignore-start -->

- Are familiar with Python and/or the command line
- Have the following tools installed:
    - Python >= 3.10
    - The `octue` python library / CLI (see [installation instructions](../installation.md))

<!-- prettier-ignore-end -->

## Authentication

No authentication is needed to run the example data service. To authenticate for real data services, see
[authentication instructions](../authentication.md).

## Run your first analysis

!!! info

    In Twined, sending input data to a service is called "asking a question". The service will run an analysis on the
    question and send back any output data - this is called called "receiving an answer".

### Ask a question

The following command asks a question to the local example data service, which calculates the first `n` values of the
[Fibonacci sequence](https://en.wikipedia.org/wiki/Fibonacci_sequence).

=== "CLI"

    ```shell
    octue twined question ask example/service:latest --input-values='{"n": 10}'
    ```

    !!! tip

        To ask a question to a real data service, just specify its ID:

        ```shell
        octue twined question ask some-org/a-service:1.2.0 --input-values='{"n": 10}'
        ```

=== "Python"

    ```python
    from octue.twined.resources import Child

    child = Child(
        id="example/service:latest",
        backend={
            "name": "GCPPubSubBackend",
            "project_id": "example",
        },
    )

    answer, question_uuid = child.ask(input_values={"n": 10})
    ```

    !!! info

        A child is a Twined service you ask a question to, in the sense of child and parent nodes in a tree. This only
        becomes important when services use other Twined services as part of their analysis, forming a tree of services.

    !!! tip

        To ask a question to a real data service, specify its ID and project ID e.g. `some-org/real-service:1.2.0`
        instead of `example/service:latest`.

### Receive an answer

=== "CLI"

    The output is automatically written to the command line. It contains log messages followed by the answer as
    [JSON](https://en.wikipedia.org/wiki/JSON):

    ```text
    [2025-10-28 15:36:52,377 | INFO | octue.twined.resources.example] Starting Fibonacci sequence calculation.
    [2025-10-28 15:36:52,377 | INFO | octue.twined.resources.example] Finished Fibonacci sequence calculation.
    {"kind": "result", "output_values": {"fibonacci": [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]}, "output_manifest": null}
    ```

    !!! tip

        You can pipe the output JSON into other CLI tools or redirect it to a file:

        ```shell
        # Format the result using the `jq` command line tool
        octue twined question ask example/service:latest --input-values='{"n": 10}' | jq

        # Store the result in a file
        octue twined question ask example/service:latest --input-values='{"n": 10}' > result.json
        ```

=== "Python"

    ```python
    answer

    >>> {
        "kind": "result",
        "output_values": {"fibonacci": [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]},
        "output_manifest": None,
    }
    ```

## Next steps

!!! success

    Congratulations on running your first analysis! For additional information, check out the following resources:

    - [Create your own data service](creating_services.md)
    - [Set up infrastructure to host your data service(s) in the cloud](managing_infrastructure.md)
    - Run a data service locally
    - See the library and CLI reference
    - [Get support](../support.md)
