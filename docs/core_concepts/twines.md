# Anatomy of the twine file

The main point of a twine file (`twine.json`) is to enable engineers and scientists to easily (and rigorously) define a
Twined service.

Adding a twine means you can:

- Communicate to you, a colleague, another service or machine what data is required by the service
- Deploy services automatically with a provider like [Octue](https://www.octue.com)

Here, we describe the parts of a twine and what they mean.

!!! tip

    To just get started building a twine, check out the [quickstart](../twine_file_quickstart).

## Strands

A twine has several sections, called strands. Each defines a different kind of data required (or produced) by the service.

| Strand                 | Describes the service's requirements for...                                          |
| ---------------------- | ------------------------------------------------------------------------------------ |
| Configuration values   | Data, in JSON form, used for configuration of the service                            |
| Configuration manifest | Files/datasets required by the service at configuration/startup                      |
| Input values           | Data, in JSON form, passed to the service in order to trigger an analysis            |
| Input manifest         | Files/datasets passed with input values to trigger an analysis                       |
| Output values          | Data, in JSON form, that will be produced by the service (in response to inputs)     |
| Output manifest        | Files/datasets that will be produced by the service (in response to inputs)          |
| Credentials            | Credentials that are required by the service in order to access third party services |
| Children               | Other twins, access to which are required for this service to function               |
| Monitors               | Visual and progress outputs from an analysis                                         |

## Twine file schema

Because the twine itself is a JSON file with a strict structure, there's a schema to make sure it's correctly
written (a "schema of a schema", or metaschema). We don't need to think about it too much here, but it's
[here for reference](https://github.com/octue/octue-sdk-python/blob/main/octue/twined/schema/twine_schema.json).

The first thing Twined always does is check that the `twine.json` file itself is valid, and give you a descriptive error
if it isn't.
