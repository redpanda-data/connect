---
id: joining-streams
title: Joining Streams
description: How to hydrate documents by joining multiple streams.
---

This cookbook demonstrates how to merge JSON events from parallel streams using content based rules and a [cache][caches] of your choice.

The imaginary problem we are going to solve is hydrating a feed of article comments with information from their parent articles. We will be consuming and writing to Kafka, but the example works with any [input][inputs] and [output][outputs] combination.

Articles are received over the topic `articles` and look like this:

```json
{
  "type": "article",
  "article": {
    "id": "123foo",
    "title": "Dope article",
    "content": "this is a totally dope article"
  },
  "user": {
    "id": "user1"
  }
}
```

Comments can either be posted on an article or a parent comment, are received over the topic `comments`, and look like this:

```json
{
  "type": "comment",
  "comment": {
    "id": "456bar",
    "parent_id": "123foo",
    "content": "this article sucks"
  },
  "user": {
    "id": "user2"
  }
}
```

Our goal is to end up with a single stream of comments, where information about the root article of the comment is attached to the event. The above comment should exit our pipeline looking like this:

```json
{
  "type": "comment",
  "comment": {
    "id": "456bar",
    "parent_id": "123foo",
    "content": "this article sucks"
  },
  "article": {
    "title": "Dope article",
    "content": "this is a totally dope article"
  },
  "user": {
    "id": "user2"
  }
}
```

In order to achieve this we will need to cache articles as they pass through our pipelines and then retrieve them for each comment passing through. Since the parent of a comment might be another comment we will also need to cache and retrieve comments in the same way.

## Caching Articles

Our first pipeline is very simple, we just consume articles, reduce them to only the fields we wish to cache, and then cache them. If we receive the same article multiple times we're going to assume it's okay to overwrite the old article in the cache.

In this example I'm targeting Redis, but you can choose any of the supported [cache targets][caches]. The TTL of cached articles is set to one week.

```yaml
input:
  kafka:
    addresses: [ TODO ]
    topics: [ articles ]
    consumer_group: benthos_articles_group

pipeline:
  processors:
    # Reduce document into only fields we wish to cache.
    - bloblang: 'article = article'

    # Store reduced articles into our cache.
    - cache:
        operator: set
        resource: hydration_cache
        key: '${!json("article.id")}'
        value: '${!content()}'

# Drop all articles after they are cached.
output:
  drop: {}

cache_resources:
  - label: hydration_cache
    redis:
      url: TODO
      default_ttl: 168h
```

## Hydrating Comments

Our second pipeline consumes comments, caches them in case a subsequent comment references them, obtains its parent (article or comment), and attaches the root article to the event before sending it to our output topic `comments_hydrated`.

In this config we make use of the [`branch`][processor.branch] processor as it allows us to reduce documents into smaller maps for caching and gives us greater control over how results are mapped back into the document.

```yaml
input:
  kafka:
    addresses: [ TODO ]
    topics: [ comments ]
    consumer_group: benthos_comments_group

pipeline:
  processors:
    # Perform both hydration and caching within a for_each block as this ensures
    # that a given message of a batch is cached before the next message is
    # hydrated, ensuring that when a message of the batch has a parent within
    # the same batch hydration can still work.
    - for_each:
      # Attempt to obtain parent event from cache (if the ID exists).
      - branch:
          request_map: 'root = this.comment.parent_id | deleted()'
          processors:
            - cache:
                operator: get
                resource: hydration_cache
                key: '${!content()}'
          # And if successful copy it into the field `article`.
          result_map: 'root.article = this.article'
      
      # Reduce comment into only fields we wish to cache.
      - branch:
          request_map: |
            root.comment.id = this.comment.id
            root.article = this.article
          processors:
            # Store reduced comment into our cache.
            - cache:
                operator: set
                resource: hydration_cache
                key: '${!json("comment.id")}'
                value: '${!content()}'
          # No `result_map` since we don't need to map into the original message.

# Send resulting documents to our hydrated topic.
output:
  kafka:
    addresses: [ TODO ]
    topic: comments_hydrated

cache_resources:
  - label: hydration_cache
    redis:
      url: TODO
      default_ttl: 168h
```

This pipeline satisfies our basic needs but errors aren't handled at all, meaning intermittent cache connectivity problems that span beyond our cache retries will result in failed documents entering our `comments_hydrated` topic. This is also the case if a comment arrives in our pipeline before its parent.

There are [many patterns for error handling][error-handling] to choose from in Benthos. In this example we're going to introduce a delayed retry queue as it enables us to reprocess failed documents after a grace period, which is isolated from our main pipeline.

## Adding a Retry Queue

Our retry queue is going to be another topic called `comments_retried`. Since most errors are related to time we will delay retry attempts by storing the current timestamp after a failed request as a metadata field.

We will use an input [`broker`][input.broker] so that we can consume both the `comments` and `comments_retry` topics in the same pipeline.

Our config (omitting the caching sections for brevity) now looks like this:

```yaml
input:
  broker:
    inputs:
      - kafka:
          addresses: [ TODO ]
          topics: [ comments ]
          consumer_group: benthos_comments_group

      - kafka:
          addresses: [ TODO ]
          topics: [ comments_retry ]
          consumer_group: benthos_comments_group

        processors:
          - for_each:
            # Calculate time until next retry attempt and sleep for that duration.
            # This sleep blocks the topic 'comments_retry' but NOT 'comments',
            # because both topics are consumed independently and these processors
            # only apply to the 'comments_retry' input.
            - sleep:
                duration: '${! 3600 - ( timestamp_unix() - meta("last_attempted").number() ) }s'

pipeline:
  processors:
    - try:
      - for_each:
        # Attempt to obtain parent event from cache.
        - branch:
            {} # Omitted

        # Reduce document into only fields we wish to cache.
        - branch:
            {} # Omitted

      # If we've reached this point then both processors succeeded.
      - bloblang: 'meta output_topic = "comments_hydrated"'

    - catch:
        # If we reach here then a processing stage failed.
        - bloblang: |
            meta output_topic = "comments_retry"
            meta last_attempted = timestamp_unix()

# Send resulting documents either to our hydrated topic or the retry topic.
output:
  kafka:
    addresses: [ TODO ]
    topic: '${!meta("output_topic")}'

cache_resources:
  - label: hydration_cache
    redis:
      url: TODO
      default_ttl: 168h
```

You can find a full example [in the project repo][full-example], and with this config we can deploy as many instances of Benthos as we need as the partitions will be balanced across the consumers.

[caches]: /docs/components/caches/about
[inputs]: /docs/components/inputs/about
[input.broker]: /docs/components/inputs/broker
[outputs]: /docs/components/outputs/about
[error-handling]: /docs/configuration/error_handling
[processor.branch]: /docs/components/processors/branch
[full-example]: https://github.com/benthosdev/benthos/blob/master/config/examples/joining_streams.yaml