---
title: sample
type: processor
---

```yaml
sample:
  retain: 10
  seed: 0
```

Retains a randomly sampled percentage of message batches (0 to 100) and drops
all others. The random seed is static in order to sample deterministically, but
can be set in config to allow parallel samples that are unique.


