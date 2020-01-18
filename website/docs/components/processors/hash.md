---
title: hash
type: processor
---

```yaml
hash:
  algorithm: sha256
  parts: []
```

Hashes messages according to the selected algorithm. Supported algorithms are:
sha256, sha512, sha1, xxhash64.

This processor is mostly useful when combined with the
[`process_field`](process_field) processor as it allows you to hash a
specific field of a document like this:

``` yaml
# Hash the contents of 'foo.bar'
process_field:
  path: foo.bar
  processors:
  - hash:
      algorithm: sha256
```


