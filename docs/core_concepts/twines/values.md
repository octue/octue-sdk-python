# Values strands

The `configuration_values_schema`, `input_values_schema` and
`output_values_schema` strands are _values-based_, meaning the data that
matches these strands is in JSON form.

Each of these strands is a _json schema_ which describes that data.

## Configuration values strand

This strand is a `configuration_values_schema`, that is used to check
validity of any `configuration_values` data supplied to the twin at
startup.

The configuration values strand is generally used to define control
parameters relating to what the twin should do, or how it should
operate.

For example, should it produce output images as low resolution PNGs or
as SVGs? How many iterations of a fluid flow solver should be used? What
is the acceptable error level on a classifier algorithm?

### Example

This _twine_ contains an example `configuration_values_schema` with one
control parameter.

```json
{
  "configuration_values_schema": {
    "title": "The example configuration form",
    "description": "The Configuration Values Strand of an example twine",
    "type": "object",
    "properties": {
      "n_iterations": {
        "description": "An example of an integer configuration variable, called 'n_iterations'.",
        "type": "integer",
        "minimum": 1,
        "maximum": 10,
        "default": 5
      }
    }
  }
}
```

Matching `configuration_values` data could look like this:

```json
{
  "n_iterations": 8
}
```

## Input values strand

This strand is an `input_values_schema`, that is used to check validity
of `input_values` data supplied to the twin at the beginning of an
analysis task.

The input values strand is generally used to define actual data which
will be processed by the twin. Sometimes, it may be used to define
control parameters specific to an analysis.

For example, if a twin cleans and detects anomalies in a 10-minute
timeseries of 1Hz data, the `input_values` might contain an array of
data and a list of corresponding timestamps. It may also contain a
control parameter specifying which algorithm is used to do the
detection.

### Example

This _twine_ contains an example `input_values_schema` with one input value, which marked as required.

```json
{
    "input_values_schema": {
        "title": "Input Values",
        "description": "The input values strand of an example twine, with a required height value",
        "type": "object",
        "properties": {
            "height": {
                "description": "An example of an integer value called 'height'",
                "type": "integer",
                "minimum": 2
            }
        },
        "required": ["height"]
    },
```

Matching `input_values` data could look like this:

```json
{
  "height": 13
}
```

## Output values strand

This strand is an `output_values_schema`, that is used to check results
(`output_values`) computed during an analysis. This ensures that the
application wrapped up within the _twine_ is operating correctly, and
enables other twins/services or the end users to see what outputs they
will get.

For example,if a twin cleans and detects anomalies in a 10-minute
timeseries of 1Hz data, the `output_values` might contain an array of
data interpolated onto regular timestamps, with missing values filled in
and a list of warnings where anomalies were found.

### Example

To be added.

!!! tip

    More examples are available in the [GitHub repository](https://github.com/octue/octue-sdk-python/tree/main/octue/twined/examples).
