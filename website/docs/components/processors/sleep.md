---
title: sleep
type: processor
---

```yaml
sleep:
  duration: 100us
```

Sleep for a period of time specified as a duration string. This processor will
interpolate functions within the `duration` field, you can find a list
of functions [here](/docs/configuration/interpolation#functions).

This processor executes once per message batch. In order to execute once for
each message of a batch place it within a
[`for_each`](for_each) processor:

``` yaml
for_each:
- sleep:
    duration: ${!metadata:sleep_for}
```


