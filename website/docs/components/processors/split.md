---
title: split
type: processor
---

```yaml
split:
  byte_size: 0
  size: 1
```

Breaks message batches (synonymous with multiple part messages) into smaller
batches. The size of the resulting batches are determined either by a discrete
size or, if the field `byte_size` is non-zero, then by total size in
bytes (which ever limit is reached first).

If there is a remainder of messages after splitting a batch the remainder is
also sent as a single batch. For example, if your target size was 10, and the
processor received a batch of 95 message parts, the result would be 9 batches of
10 messages followed by a batch of 5 messages.


