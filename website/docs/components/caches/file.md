---
title: file
type: cache
---

```yaml
file:
  directory: ""
```

The file cache stores each item in a directory as a file, where an item ID is
the path relative to the configured directory.

This type currently offers no form of item expiry or garbage collection, and is
intended to be used for development and debugging purposes only.


