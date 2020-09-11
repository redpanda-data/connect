---
title: Conditions
sidebar_label: About
---

Conditions are boolean queries that can be executed based on the contents of a message, some [processors][processors] such as [`switch`][processor.switch] use conditions for expressing their logic. There are many options but the most powerful type is the [`bloblang` condition][condition.bloblang].

Conditions themselves can modify ([`not`][condition.not]) and combine ([`and`][condition.and], [`or`][condition.or]) other conditions, and can therefore be used to create complex boolean expressions.

The format of a condition is similar to other Benthos types:

```yml
condition:
  bloblang: meta("kafka_topic") == "foo"
```

And is usually found as the child of a processor:

```yaml
pipeline:
  processors:
    - switch:
      - condition:
          bloblang: meta("kafka_topic") == "foo"
        processors:
          - resource: foo
      - processors:
          - resource: bar
```

### Batching and Multipart Messages

All conditions can be applied to a multipart message, which is synonymous with a batch. Some conditions target a specific part of a message batch, and require you specify the target index with the field `part`.

Part indexes can be negative, and if so the part will be selected from the end counting backwards starting from -1. E.g. if part = -1 then the selected part will be the last part of the message, if part = -2 then the part before the last element with be selected, and so on.

### Reusing Conditions

Sometimes large chunks of logic are reused across processors, or nested multiple times as branches of a larger condition. It is possible to avoid writing duplicate condition configs by using the [resource condition][condition.resource].

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="conditions"></ComponentSelect>

[processors]: /docs/components/processors/about
[processor.switch]: /docs/components/processors/switch
[condition.bloblang]: /docs/components/conditions/bloblang
[condition.and]: /docs/components/conditions/and
[condition.or]: /docs/components/conditions/or
[condition.not]: /docs/components/conditions/not
[condition.resource]: /docs/components/conditions/resource
