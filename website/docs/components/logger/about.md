---
title: Logger
---

Benthos logging prints to stdout (or stderr if your output is stdout), and is formatted as [logfmt](https://brandur.org/logfmt) by default.

```yaml
logger:
  level: INFO
  format: logfmt
  add_timestamp: false
  static_fields:
    '@service': benthos
```

Possible log levels are `OFF`, `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` and `ALL`.

Possible log formats are `logfmt` and `json`.
