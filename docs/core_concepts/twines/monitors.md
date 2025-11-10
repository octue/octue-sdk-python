# Monitor message strand

The `monitor_message_schema` strand is _values-based_ meaning the data
that matches the strand is in JSON form. It is a _JSON schema_ which
describes a monitor message.

Monitor messages can include values for health and progress monitoring of the twin, for example
percentage progress, iteration number, and status - perhaps even
residuals graphs for a converging calculation. Broadly speaking, this
should be user-facing information. This kind of monitoring data can be in a suitable form for display on a dashboard.

```json
{
  "monitor_message_schema": {
    "type": "object",
    "properties": {
      "my_property": {
        "type": "number"
      }
    },
    "required": ["my_property"]
  }
}
```
