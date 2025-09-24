# Getting started

Twined is a python framework for creating and using data services and digital twins.

This guide walks you through using an example Twined data service locally (the process for using a real one is almost
identical).

By the end, you will be able to use the Twined CLI to run an analysis on a data service, sending it input data and receiving its output.

## Prerequisites

Before you begin, ensure you:

<!-- prettier-ignore-start -->

- Are familiar with Python
- Have the following tools installed:
    - Python >= 3.10
    - The `octue` python library (see [installation instructions](installation.md))

<!-- prettier-ignore-end -->

## Authentication

No authentication is needed to run the example data service. To authenticate for real data services, see [here](authentication.md).

## Run your first analysis

### Request

The following shell command runs an analysis in the local example data service:

```shell
octue twined question ask example --input-values='{"some": "data"}'
```

!!! info

    To ask a question to a real data service, the command is almost the same:

    ```shell
    octue twined question ask remote --input-values='{"some": "data"}'
    ```

### Response

The response contains log messages in `stderr` followed by the result as JSON in `stdout`:

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

## Next steps

Congratulations on running your first analysis! For additional information, check out the following resources:

- {Link to other relevant documentation such as API Reference}
- {Link to other features that are available in the API}
- {Provide links to additional tutorials and articles about the API}
- {Provide links to community and support groups, FAQs, troubleshooting guides, etc.}
