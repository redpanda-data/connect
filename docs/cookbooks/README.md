Cookbooks
=========

This is a collection of Benthos snippets demonstrating common but more advanced
patterns in stream processing.

### [Duplicate Monitoring](./duplicate-monitoring.md)

Benthos has a [deduplication processor](../processors/README.md#dedupe) which
makes removing duplicates from a stream easy. However, sometimes we might only
be interested in detecting duplicates without mutating the stream. This cookbook
demonstrates how to passively count duplicates and expose them via metrics
aggregators.

TODO: Joining streams of JSON documents
TODO: Parallel enrichment of JSON documents