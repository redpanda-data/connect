JSON References
===============

This directory showcases some of the capabilities of using
[JSON References][json-refs] in order to build modular Benthos configurations
that can be run and tested in isolation.

## Modules

The first config files to check out are within [`./modules`](./modules). These
config files each perform a single action:

### [`./modules/group.yaml`](./modules/group.yaml)

Groups JSON documents by a field at the path `document.type`.

### [`./modules/filter.yaml`](./modules/filter.yaml)

Removes JSON documents where the content does not contain the string `2`.

### [`./modules/mutate.yaml`](./modules/mutate.yaml)

Modifies JSON documents by converting the entire contents of the field
`document.content` to upper case.

#### Run

Each of these modules are functioning Benthos configuration files that you can
run:

``` sh
$ echo '{"document":{"type":"foo","content":"foo type 1"}}
{"document":{"type":"bar","content":"bar type 1"}}
{"document":{"type":"foo","content":"foo type 2"}}
{"document":{"type":"baz","content":"baz type 1"}}
{"document":{"type":"baz","content":"baz type 2"}}
{"document":{"type":"bar","content":"bar type 2"}}' | benthos -c ./modules/group.yaml
```

And it's also possible to use the processors defined in these modules using a
reference:

``` yaml
pipeline:
  processors:
  - $ref: ./modules/filter.yaml
```

## Using References

Finally, let's take a look at our [main configuration](./pipeline.yaml), which
uses JSON references in order to combine a set of our modules. The config itself
is tiny:

``` yaml
pipeline:
  processors:
  - $ref: ./modules/group.yaml#/pipeline/processors/0
  - $ref: ./modules/filter.yaml#/pipeline/processors/0
  - $ref: ./modules/mutate.yaml#/pipeline/processors/0
```

And in this case we have simply chained the processor from each module. We might
instead wish to select the module that is referenced at the moment when we
execute Benthos, in which case we can set the reference using environment
variables:

``` yaml
pipeline:
  processors:
  # Always group
  - $ref: ./modules/group.yaml#/pipeline/processors/0

  # Second processor set via environment variable (filter by default)
  - $ref: ./modules/${MODULE:filter}.yaml#/pipeline/processors/0
```

And then we can execute this config with:

``` sh
$ MODULE=mutate benthos -c ./pipeline.yaml
```

[json-refs]: https://tools.ietf.org/html/draft-pbryan-zyp-json-ref-03