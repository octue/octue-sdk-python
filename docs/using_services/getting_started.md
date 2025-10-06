# Getting started

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
[authentication instructions](authentication.md).

## Run your first analysis

!!! info

    In Twined, sending input data to a service is called "asking a question". The service will run an analysis on the
    question and send back any output data - this is called called "receiving an answer".

### Ask a question

The following command asks a question to the local example data service.

=== "CLI"

    ```shell
    octue twined question ask example/service:latest --input-values='{"some": "data"}'
    ```

    !!! tip

        To ask a question to a real data service, just specify its ID:

        ```shell
        octue twined question ask some-org/a-service:1.2.0 --input-values='{"some": "data"}'
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

    answer, question_uuid = child.ask(input_values={"some": "data"})
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
    [2025-09-23 18:20:13,513 | WARNING | octue.resources.dataset] <Dataset('cleaned_met_mast_data')> is empty at instantiation time (path 'cleaned_met_mast_data').
    [2025-09-23 18:20:13,649 | INFO | app] [fb85f10d-4428-4239-9543-f7650104b43c] Starting clean up of files in <Manifest(8ead7669-8162-4f64-8cd5-4abe92509e17)>
    [2025-09-23 18:20:13,649 | INFO | app] [fb85f10d-4428-4239-9543-f7650104b43c] Averaging window set to 600s
    [2025-09-23 18:20:13,673 | INFO | octue.twined.resources.analysis] [fb85f10d-4428-4239-9543-f7650104b43c] The analysis didn't produce output values.
    [2025-09-23 18:20:13,673 | INFO | octue.twined.resources.analysis] [fb85f10d-4428-4239-9543-f7650104b43c] The analysis produced an output manifest.
    [2025-09-23 18:20:13,673 | INFO | octue.twined.resources.analysis] [fb85f10d-4428-4239-9543-f7650104b43c] No output location was set in the service configuration - can't upload output datasets.
    [2025-09-23 18:20:13,726 | INFO | octue.twined.resources.analysis] [fb85f10d-4428-4239-9543-f7650104b43c] Validated outputs against the twine.
    {"kind": "result", "output_values": {"some": "output", "heights": [1, 2, 3, 4, 5]}, "output_manifest": {"id": "2e1fb3e4-2f86-4eb2-9c2f-5785d36c6df9", "name": null, "datasets": {"cleaned_met_mast_data": "/var/folders/9p/25hhsy8j4wv66ck3yylyz97c0000gn/T/tmps_qcb4yw"}}}
    ```

    !!! tip

        You can pipe the output JSON into other CLI tools or redirect it to a file:

        ```shell
        # Format the result using the `jq` command line tool
        octue twined question ask example/service:latest --input-values='{"some": "data"}' | jq

        # Store the result in a file
        octue twined question ask example/service:latest --input-values='{"some": "data"}' > result.json
        ```

=== "Python"

    ```python
    answer

    >>> {
        "kind": "result",
        "output_values": {"some": "output", "heights": [1, 2, 3, 4, 5]},
        "output_manifest": {"id": "2e1fb3e4-2f86-4eb2-9c2f-5785d36c6df9", "name": null, "datasets": {"cleaned_met_mast_data": "/var/folders/9p/25hhsy8j4wv66ck3yylyz97c0000gn/T/tmps_qcb4yw"}},
    }
    ```

## Next steps

Congratulations on running your first analysis! For additional information, check out the following resources:

- [Set up infrastructure for a data service(s) using Terraform](../managing_infrastructure/getting_started.md)
- [Create a data service](../creating_services/getting_started.md)
- Run a data service locally
- See the library and CLI reference
- [Get support](../support.md)
