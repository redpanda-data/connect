---
title: process_dag
type: processor
---

```yaml
process_dag: {}
```

A processor that manages a map of `process_map` processors and
calculates a Directed Acyclic Graph (DAG) of their dependencies by referring to
their postmap targets for provided fields and their premap targets for required
fields.

The names of workflow stages may only contain alphanumeric, underscore and dash
characters (they must match the regular expression `[a-zA-Z0-9_-]+`).

The DAG is then used to execute the children in the necessary order with the
maximum parallelism possible. You can read more about workflows in Benthos
[in this document](/docs/configuration/workflows).

The field `dependencies` is an optional array of fields that a child
depends on. This is useful for when fields are required but don't appear within
a premap such as those used in conditions.

This processor is extremely useful for performing a complex mesh of enrichments
where network requests mean we desire maximum parallelism across those
enrichments.

For example, if we had three target HTTP services that we wished to enrich each
document with - foo, bar and baz - where baz relies on the result of both foo
and bar, we might express that relationship here like so:

``` yaml
process_dag:
  foo:
    premap:
      .: .
    processors:
    - http:
        request:
          url: http://foo/enrich
    postmap:
      foo_result: .
  bar:
    premap:
      .: msg.sub.path
    processors:
    - http:
        request:
          url: http://bar/enrich
    postmap:
      bar_result: .
  baz:
    premap:
      foo_obj: foo_result
      bar_obj: bar_result
    processors:
    - http:
        request:
          url: http://baz/enrich
    postmap:
      baz_obj: .
```

With this config the DAG would determine that the children foo and bar can be
executed in parallel, and once they are both finished we may proceed onto baz.


