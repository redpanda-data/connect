---
title: read_until
type: input
---

```yaml
read_until:
  condition:
    text:
      arg: ""
      operator: equals_cs
      part: 0
    type: text
  input: {}
  restart_input: false
```

Reads from an input and tests a condition on each message. Messages are read
continuously while the condition returns false, when the condition returns true
the message that triggered the condition is sent out and the input is closed.
Use this type to define inputs where the stream should end once a certain
message appears.

Sometimes inputs close themselves. For example, when the `file` input
type reaches the end of a file it will shut down. By default this type will also
shut down. If you wish for the input type to be restarted every time it shuts
down until the condition is met then set `restart_input` to `true`.

### Metadata

A metadata key `benthos_read_until` containing the value `final` is
added to the first part of the message that triggers to input to stop.


