---
id: filtering
title: Filtering and Sampling
description: Configure Benthos to conditionally drop messages.
---

Events are like eyebrows, sometimes it's best to just get rid of them. Filtering events in Benthos is both easy and flexible, this cookbook demonstrates a few different types of filtering you can do. All of these examples make use of the [`bloblang` processor][processors.bloblang] but shouldn't require any prior knowledge.

## The Basic Filter

Dropping events with Bloblang is done by mapping the function `deleted()` to the `root` of the mapped document. To remove all events indiscriminantly you can simply do:

```yaml
pipeline:
  processors:
  - bloblang: root = deleted()
```

But that's most likely not what you want. We can instead only delete an event under certain conditions with a [`match` expression][bloblang.match]:

```yaml
pipeline:
  processors:
  - bloblang: |
      root = match {
        meta("topic").or("") == "foo" ||
        doc.type.or("") == "bar" ||
        doc.urls.contains("https://www.benthos.dev/").catch(false) => deleted()
      }
```

The above config removes any events where:

- The metadata field `topic` is equal to `foo`
- The event field `doc.type` (a string) is equal to `bar`
- The event field `doc.urls` (an array) contains the string `https://www.benthos.dev/`

Events that do not match any of these conditions will remain unchanged.

## Sample Events

Another type of filter we might want is a sampling filter, we can do that with a random number generator:

```yaml
pipeline:
  processors:
  - bloblang: |
      # Drop 50% of documents randomly
      root = match {
        random_int() % 2 == 0 => deleted()
      }
```

We can also do this in a deterministic way by hashing events and using the hash as our seed:

```yaml
pipeline:
  processors:
  - bloblang: |
      # Drop 10% of documents deterministically (same docs filtered each run)
      root = match {
        random_int(content().hash("xxhash64").number()) % 10 == 0 => deleted()
      }
```

[processors.bloblang]: /docs/components/processors/bloblang
[bloblang.match]: /docs/guides/bloblang/about##pattern-matching