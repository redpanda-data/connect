---
title: resource
type: processor
---

```yaml
resource: ""
```

Resource is a processor type that runs a processor resource by its name. This
processor allows you to run the same configured processor resource in multiple
places.

Resource processors also have the advantage of name based metrics and logging.
For example, the config:

``` yaml
pipeline:
  processors:
    - jmespath:
        query: foo
```

Is equivalent to:

``` yaml
pipeline:
  processors:
    - resource: foo_proc

resources:
  processors:
    foo_proc:
      jmespath:
        query: foo
```

But now the metrics path of the JMESPath processor will be
`resources.processors.foo_proc`, this way of flattening observability
labels becomes more useful as configs get larger and more nested.


