---
title: resource
type: condition
---

```yaml
resource: ""
```

Resource is a condition type that runs a condition resource by its name. This
condition allows you to run the same configured condition resource in multiple
processors, or as a branch of another condition.

For example, let's imagine we have two outputs, one of which only receives
messages that satisfy a condition and the other receives the logical NOT of that
same condition. In this example we can save ourselves the trouble of configuring
the same condition twice by referring to it as a resource, like this:

``` yaml
output:
  broker:
    pattern: fan_out
    outputs:
    - foo:
        processors:
        - filter:
            type: resource
            resource: foobar
    - bar:
        processors:
        - filter:
            not:
              type: resource
              resource: foobar
resources:
  conditions:
    foobar:
      text:
        operator: equals_cs
        part: 1
        arg: filter me please
```


