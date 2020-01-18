---
title: switch
type: processor
---

```yaml
switch: []
```

Switch is a processor that lists child case objects each containing a condition
and processors. Each batch of messages is tested against the condition of each
child case until a condition passes, whereby the processors of that case will be
executed on the batch.

Each case may specify a boolean `fallthrough` field indicating whether
the next case should be executed after it (the default is `false`.)

A case takes this form:

``` yaml
- condition:
    type: foo
  processors:
  - type: foo
  fallthrough: false
```

In order to switch each message of a batch individually use this processor with
the [`for_each`](for_each) processor.

You can find a [full list of conditions here](/docs/components/conditions/about).


