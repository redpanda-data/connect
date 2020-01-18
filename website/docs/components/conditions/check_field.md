---
title: check_field
type: condition
---

```yaml
check_field:
  condition: {}
  parts: []
  path: ""
```

Extracts the value of a field identified via [dot path](/docs/configuration/field_paths)
within messages (currently only JSON format is supported) and then tests the
extracted value against a child condition.


