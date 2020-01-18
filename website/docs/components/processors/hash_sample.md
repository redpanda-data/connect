---
title: hash_sample
type: processor
---

```yaml
hash_sample:
  parts:
  - 0
  retain_max: 10
  retain_min: 0
```

Retains a percentage of message batches deterministically by hashing selected
messages and checking the hash against a valid range, dropping all others.

For example, setting `retain_min` to `0.0` and `remain_max` to `50.0`
results in dropping half of the input stream, and setting `retain_min`
to `50.0` and `retain_max` to `100.1` will drop the _other_ half.

In order to sample individual messages of a batch use this processor with the
[`for_each`](for_each) processor.


