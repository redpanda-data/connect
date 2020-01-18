---
title: Logger
---

Benthos logging prints to stdout (or stderr if your output is stdout), and is formatted as JSON by default.

```yaml
logger:
  prefix: benthos
  level: INFO
  add_timestamp: true
  json_format: true
  static_fields:
    '@service': benthos
```

Possible log levels are `OFF`, `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` and `ALL`,