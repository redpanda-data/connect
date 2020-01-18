---
title: avro
type: processor
---

```yaml
avro:
  encoding: textual
  operator: to_json
  parts: []
  schema: ""
```

EXPERIMENTAL: This processor is considered experimental and is therefore subject
to change outside of major version releases.

Performs Avro based operations on messages based on a schema. Supported encoding
types are textual, binary and single.

### Operators

#### `to_json`

Converts Avro documents into a JSON structure. This makes it easier to
manipulate the contents of the document within Benthos. The encoding field
specifies how the source documents are encoded.

#### `from_json`

Attempts to convert JSON documents into Avro documents according to the
specified encoding.


