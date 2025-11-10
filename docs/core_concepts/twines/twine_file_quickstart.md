# Twine file quickstart

Let's say we want a service that accepts two values, uses them to make a calculation, then gives the result. Anyone
connecting to the service will need to know what values it requires, and what it responds with.

First, create a blank text file called `twine.json`. First, we'll give the service a title and description. Paste in the
following:

```json
{
  "title": "My first Twined service... of an atomising discombobulator",
  "description": "A simple example... estimates the `foz` value of an atomising discombobulator."
}
```

Now, let's define an input values strand to specify what values are required by the service. For this we use a JSON
schema. Add the `input_values` field so the twine looks like this:

```json
{
  "title": "My first Twined service",
  "description": "A simple example to build on...",
  "input_values_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Input values schema for my first Twined service",
    "description": "These values are supplied to the service by another program (often over a websocket, depending on your integration provider). So as these values change, the service can reply with an update.",
    "type": "object",
    "properties": {
      "foo": {
        "description": "The foo value... speed of the discombobulator's input bobulation module, in m/s",
        "type": "number",
        "minimum": 10,
        "maximum": 500
      },
      "baz": {
        "description": "The baz value... period of the discombobulator's recombulation unit, in s",
        "type": "number",
        "minimum": 0,
        "maximum": 1000
      }
    }
  }
}
```

Finally, let's add an output values strand showing what kind of data is returned by the service:

```json
{
    ...
    "output_values_schema": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Output values schema for my first Twined service",
        "description": "The service will output data that matches this schema",
        "type": "object",
        "properties": {
            "foz": {
                "description": "Estimate of the foz value... efficiency of the discombobulator in %",
                "type": "number",
                "minimum": 10,
                "maximum": 500
            }
        }
    }
}
```

## Load the twine

Twined provides a `Twine` class to load a twine from a file or a JSON string. The loading process checks the twine
itself is valid. It's as simple as:

```python
from octue.twined import Twine

my_twine = Twine(source='twine.json')
```

## Validate some inputs

Say we have some JSON that we want to parse and validate to make sure it matches what's required for input values.

```python
my_input_values = my_twine.validate_input_values(source='{"foo": 30, "baz": 500}')
```

You can read the values from a file too. Paste the following into a file named `input_values.json`:

```json
{
  "foo": 30,
  "baz": 500
}
```

Then parse and validate directly from the file:

```py
my_input_values = my_twine.validate_input_values(source="input_values.json")
```
