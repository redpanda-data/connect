Cookbooks
=========

This is a collection of Benthos snippets demonstrating common but more advanced patterns in stream processing.

### [Joining Streams of JSON Events](./joining-streams.md)

This cookbook demonstrates how to merge JSON events from parallel streams using content based rules and a cache of your choice.

### [Enrichment Workflows](./enrichments.md)

This cookbook demonstrates how to enrich a stream of JSON documents with HTTP services following a dynamic workflow. This method also works with AWS Lambda functions, subprocesses, etc.

### [Duplicate Monitoring](./duplicate-monitoring.md)

Benthos has a [deduplication processor](../processors/README.md#dedupe) which makes removing duplicates from a stream easy. However, sometimes we might only be interested in detecting duplicates without mutating the stream. This cookbook demonstrates how to passively count duplicates and expose them via metrics aggregators.
