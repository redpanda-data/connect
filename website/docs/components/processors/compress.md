---
title: compress
type: processor
---

```yaml
compress:
  algorithm: gzip
  level: -1
  parts: []
```

Compresses messages according to the selected algorithm. Supported compression
algorithms are: gzip, zlib, flate.

The 'level' field might not apply to all algorithms.


