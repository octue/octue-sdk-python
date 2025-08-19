# app.py file {#creating_apps}

The `app.py` file is, as you might expect, the entrypoint to your app.
It can contain any valid python including imports and use of any number
of external packages or internal/local packages.

## Structure

The `app.py` file must contain exactly one of the `octue` python app
interfaces to serve as the entrypoint to your code. These take a single
`Analysis` instance as a parameter or attribute:

- **Option 1:** A function named `run` with the following signature:

  ```python
  def run(analysis):
      """A function that uses input and configuration from an ``Analysis``
      instance and stores any output values and output manifests on it.
      It shouldn't return anything.

      :param octue.resources.Analysis analysis:
      :return None:
      """
      ...
  ```

- **Option 2:** A class named `App` with the following signature:

  ```python
  class App:
      """A class that takes an ``Analysis`` instance and any number of
      other parameters in its constructor. It can have any number of
      methods but must always have a ``run`` method with the signature
      shown below.

      :param octue.resources.Analysis analysis:
      :return None:
      """

      def __init__(self, analysis):
          self.analysis = analysis
          ...

      def run(self):
          """A method that that uses input and configuration from an
          ``Analysis`` instance and stores any output values and
          output manifests on it. It shouldn't return anything.

          :return None:
          """
          ...

      ...
  ```

## Accessing inputs and storing outputs

Your app must access configuration and input data from and store output
data on the `analysis` parameter (for function-based apps) or attribute (for
class-based apps). This allows standardised configuration/input/output
validation against the twine and interoperability of all Twined services
while leaving you freedom to do any kind of computation. To access the
data, use the following attributes on the `analysis`
parameter/attribute:

- Configuration values: `analysis.configuration_values`
- Configuration manifest: `analysis.configuration_manifest`
- Input values: `analysis.input_values`
- Input manifest: `analysis.input_manifest`
- Output values: `analysis.output_values`
- Output manifest: `analysis.output_manifest`

## Sending monitor messages

As well as sending the final result of the analysis your app produces to
the parent, you can send monitor messages as computation progresses.
This functionality can be used to update a plot or database in real time
as the analysis is in progress.

```python
def run(analysis):
   some_data = {"x": 0, "y", 0.1, "z": 2}
   analysis.send_monitor_message(data=some_data)
```

Before sending monitor messages, the `monitor_message_schema` field must
be provided in `twine.json`. For example:

```json
{
    ...
    "monitor_message_schema": {
        "x": {
            "description": "Real component",
            "type": "number"
        },
        "y": {
            "description": "Imaginary component",
            "type": "number"
        },
        "z": {
            "description": "Number of iterations before divergence",
            "type": "number",
            "minimum": 0
        }
    },
    ...
}
```

Monitor messages can also be set up to send periodically in time.

```python
def run(analysis):

    # Define a data structure whose attributes can be accessed in real
    # time as they're updated during the analysis.
    class DataStructure:
        def __init__(self):
            self.x = 0
            self.y = 0
            self.z = 0

        def as_dict(self):
            """Add a method that provides the data in the format
            required for the monitor messages.

            :return dict:
            """
            return {"x": self.x, "y": self.y, "z": self.z}

    # Create an instance of the data structure.
    my_updating_data = DataStructure()

    # Use the `as_dict` method to provide up-to-date data from the data
    # structure to send as monitor messages every 60s.
    analysis.set_up_periodic_monitor_message(
        create_monitor_message=my_updating_data.as_dict,
        period=60,
    )

    # Run long-running computations on the data structure that update its
    # "x", "y", and "z" attributes in real time. The periodic monitor
    # message will always send the current values of x, y, and z.
    some_function(my_updating_data)
```

## Finalising the analysis

When the analysis has finished, it is automatically finalised. This
means:

- The output values and manifest are validated against `twine.json` to
  ensure they're in the format promised by the service.
- If the app produced an output manifest and the `output_location` field
  is set to a cloud directory path in the app configuration, the output
  datasets are uploaded to this location.

!!! note

    You can manually call
    `analysis.finalise` if you want to upload any output datasets to a different
    location to the one specified in the service configuration. If you do
    this, the analysis will not be finalised again - make sure you only call
    it when your output data is ready. Note that the `output_location` and
    `use_signed_urls_for_output_datasets` settings in the service
    configuration are ignored if you call it manually.
