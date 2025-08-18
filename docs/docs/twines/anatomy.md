# Anatomy Of The Twine File {#anatomy}

The main point of **twined** is to enable engineers and scientists to
easily (and rigorously) define a digital twin or data service.

This is done by adding a `twine.json` file to the repository containing
your code. Adding a _twine_ means you can:

- communicate (to you or a colleague) what data is required by this
  service
- communicate (to another service / machine) what data is required
- deploy services automatically with a provider like
  [Octue](https://www.octue.com).

To just get started building a _twine_, check out the
`quick_start`{.interpreted-text role="ref"}. To learn more about twines
in general, see `about`{.interpreted-text role="ref"}. Here, we describe
the parts of a _twine_ (\"strands\") and what they mean.

## Strands

A _twine_ has several sections, called _strands_. Each defines a
different kind of data required (or produced) by the twin.

---

Strand Describes the twin\'s requirements for\...

---

`Configuration Values <values_based_strands>`{.interpreted-text Data, in JSON form, used for configuration of the
role="ref"} twin/service.

`Configuration Manifest <manifest_strands>`{.interpreted-text Files/datasets required by the twin at
role="ref"} configuration/startup

`Input Values <values_based_strands>`{.interpreted-text Data, in JSON form, passed to the twin in order
role="ref"} to trigger an analysis

`Input Manifest <manifest_strands>`{.interpreted-text role="ref"} Files/datasets passed with Input Values to
trigger an analysis

`Output Values <values_based_strands>`{.interpreted-text Data, in JSON form, that will be produced by the
role="ref"} twin (in response to inputs)

`Output Manifest <manifest_strands>`{.interpreted-text Files/datasets that will be produced by the twin
role="ref"} (in response to inputs)

`Credentials <credentials_strand>`{.interpreted-text role="ref"} Credentials that are required by the twin in
order to access third party services

`Children <children_strand>`{.interpreted-text role="ref"} Other twins, access to which are required for
this twin to function

`Monitors <monitors_strand>`{.interpreted-text role="ref"} Visual and progress outputs from an analysis

---

::: {.toctree maxdepth="1" hidden=""}
anatomy_values anatomy_manifest anatomy_credentials anatomy_monitors
anatomy_children
:::

## Twine File Schema {#twine_file_schema}

Because the `twine.json` file itself is in `JSON` format with a strict
structure, **twined** uses a schema to make that twine files are
correctly written (a \"schema-schema\", if you will, since a twine
already contains schema). Try not to think about it. But if you must,
the _twine_ schema is
[here](https://github.com/octue/twined/blob/master/twined/schema/twine_schema.json).

The first thing **twined** always does is check that the `twine.json`
file itself is valid, and give you a descriptive error if it isn\'t.

## Other External I/O {#other_external_io}

A twin might:

- GET/POST data from/to an external API,
- query/update a database,
- upload files to an object store,
- trigger events in another network, or
- perform pretty much any interaction you can think of with other
  applications over the web.

However, such data exchange may not be controllable by **twined** (which
is intended to operate at the boundaries of the twin) unless the
resulting data is returned from the twin (and must therefore be
compliant with the schema).

So, there\'s nothing for **twined** to do here, and no need for a strand
in the _twine_ file. However, interacting with third party APIs or
databases might require some credentials. See
`credentials_strand`{.interpreted-text role="ref"} for help with that.

:::: note
::: title
Note
:::

This is actually a very common scenario. For example, the purpose of the
twin might be to fetch data (like a weather forecast) from some external
API then return it in the `output_values` for use in a network of
digital twins. But its the twin developer\'s job to do the fetchin\' and
make sure the resulting data is compliant with the
`output_values_schema` (see `values_based_strands`{.interpreted-text
role="ref"}).
::::
