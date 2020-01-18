---
title: number
type: processor
---

```yaml
number:
  operator: add
  parts: []
  value: 0
```

Parses message contents into a 64-bit floating point number and performs an
operator on it. In order to execute this processor on a sub field of a document
use it with the [`process_field`](process_field) processor.

The value field can either be a number or a string type. If it is a string type
then this processor will interpolate functions within it, you can find a list of
functions [here](/docs/configuration/interpolation#functions).

For example, if we wanted to subtract the current unix timestamp from the field
'foo' of a JSON document `{"foo":1561219142}` we could use the
following config:

``` yaml
process_field:
  path: foo
  result_type: float
  processors:
  - number:
      operator: subtract
      value: "${!timestamp_unix}"
```

Value interpolations are resolved once per message batch, in order to resolve it
for each message of the batch place it within a
[`for_each`](for_each) processor.

### Operators

#### `add`

Adds a value.

#### `subtract`

Subtracts a value.


