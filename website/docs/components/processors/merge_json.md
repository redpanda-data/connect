---
title: merge_json
type: processor
---

```yaml
merge_json:
  parts: []
  retain_parts: false
```

Parses selected messages of a batch as JSON documents, attempts to merge them
into one single JSON document and then writes it to a new message at the end of
the batch. Merged parts are removed unless `retain_parts` is set to
true. The new merged message will contain the metadata of the first part to be
merged.


