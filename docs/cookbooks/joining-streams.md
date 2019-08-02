Joining Streams
===============

This cookbook demonstrates how to merge JSON events from parallel streams
using content based rules and a cache of your choice.

The imaginary problem we are going to solve is hydrating a feed of article
comments with information from their parent articles. We will be consuming and
writing to Kafka, but the example works with any [input][inputs] and
[output][outputs] combination.

Articles are received over the topic `articles` and look like this:

``` json
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

Comments can either be posted on an article or a parent comment, are received
over the topic `comments`, and look like this:

``` json
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

Our goal is to end up with a single stream of comments, where information about
the root article of the comment is attached to the event. The above comment
should exit our pipeline looking like this:

``` json
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

In order to achieve this we will need to cache articles as they pass through our
pipelines and then retrieve them for each comment passing through. Since the
parent of a comment might be another comment we will also need to cache and
retrieve comments in the same way.

## Caching Articles

Our first pipeline is very simple, we just consume articles, reduce them to only
the fields we wish to cache, and then cache them. If we receive the same article
multiple times we're going to assume it's okay to overwrite the old article in
the cache.

In this example I'm targeting Redis, but you can choose any of the supported
[cache targets][caches]. The TTL of cached articles is set to one week.

``` yaml
input:
  kafka_balanced:
    addresses:
    - TODO
    topics:
    - articles
    consumer_group: benthos_articles_group

pipeline:
  processors:
  # Reduce document into only fields we wish to cache.
  - jmespath:
      query: '{"article": article}'

  # Store reduced articles into our cache.
  - cache:
      operator: set
      cache: hydration_cache
      key: "${!json_field:article.id}"
      value: "${!content}"

# Drop all articles after they are cached.
output:
  type: drop

resources:
  caches:
    hydration_cache:
      redis:
        expiration: 168h
        retries: 3
        retry_period: 500ms
        url: TODO
```

## Hydrating Comments

Our second pipeline consumes comments, caches them in case a subsequent comment
references them, obtains its parent (article or comment), and attaches the root
article to the event before sending it to our output topic `comments_hydrated`.

In this config we make use of the [`process_map`][procmap-proc] processor as it
allows us to reduce documents into smaller maps for caching and gives us greater
control over how results are mapped back into the document.

``` yaml
input:
  kafka_balanced:
    addresses:
    - TODO
    topics:
    - comments
    consumer_group: benthos_comments_group

pipeline:
  processors:
  # Attempt to obtain parent event from cache.
  - process_map:
      premap:
        parent_id: comment.parent_id
      processors:
      - cache:
          operator: get
          cache: hydration_cache
          key: "${!json_field:parent_id}"
      postmap:
        # We only need the article section of our parent document.
        article: article
  
  # Reduce comment into only fields we wish to cache.
  - process_map:
      premap:
        comment.id: comment.id
        article: article
      processors:
      # Store reduced comment into our cache.
      - cache:
          operator: set
          cache: hydration_cache
          key: "${!json_field:comment.id}"
          value: "${!content}"
      postmap_optional:
        # Dummy map since we don't need to map the result back.
        foo: will.never.exist

# Sent resulting documents to our hydrated topic.
output:
  kafka:
    addresses:
    - TODO
    topic: comments_hydrated

resources:
  caches:
    hydration_cache:
      redis:
        expiration: 168h
        retries: 3
        retry_period: 500ms
        url: TODO
```

This pipeline satisfies our basic needs but errors aren't handled at all,
meaning intermittent cache connectivity problems that span beyond our cache
retries will result in failed documents entering our `comments_hydrated` topic.
This is also the case if a comment arrives in our pipeline before its parent.

There are [many patterns for error handling][error-handling] to choose from in
Benthos. In this example we're going to introduce a delayed retry queue as it
enables us to reprocess failed documents after a grace period, which is isolated
from our main pipeline.

## Adding a Retry Queue

Our retry queue is going to be another topic called `comments_retried`. Since
most errors are related to time we will delay retry attempts by storing the
current timestamp after a failed request as a metadata field.

We will use an input [`broker`][input-broker] so that we can consume both the
`comments` and `comments_retry` topics in the same pipeline.

Our config (omitting the caching sections for brevity) now looks like this:

``` yaml
input:
  broker:
    inputs:
    - kafka_balanced:
        addresses:
        - TODO
        topics:
        - comments
        consumer_group: benthos_comments_group

    - kafka_balanced:
        addresses:
        - TODO
        topics:
        - comments_retry
        consumer_group: benthos_comments_group

      processors:
      # Calcuate time until next retry attempt and sleep for that duration.
      - for_each:
        - awk:
            program: |
             {
               delay_for = 3600 - (timestamp_unix() - metadata_get("last_attempted"));
               if ( delay_for < 0 )
                 delay_for = 0;
               metadata_set("delay_for_s", delay_for);
             }
        - sleep:
            duration: "${!metadata:delay_for_s}s"

pipeline:
  processors:
  - try:
    # Attempt to obtain parent event from cache.
    - process_map:
        {} # Omitted

    # Reduce document into only fields we wish to cache.
    - process_map:
        {} # Omitted

    # If we've reached this point then both processors succeeded.
    - metadata:
        operator: set
        key: output_topic
        value: comments_hydrated

  - catch:
    # If we reach here then a processing stage failed.
    - metadata:
        operator: set
        key: output_topic
        value: comments_retry

    # Add current timestamp.
    - metadata:
        operator: set
        key: last_attempted
        value: ${!timestamp_unix}

# Sent resulting documents either to our hydrated topic or the retry topic.
output:
  kafka:
    addresses:
    - TODO
    topic: ${!metadata:output_topic}

resources:
  caches:
    hydration_cache:
      {} # Omitted
```

[caches]: ../caches/README.md
[inputs]: ../inputs/README.md
[input-broker]: ../inputs/README.md#broker
[outputs]: ../outputs/README.md
[error-handling]: ../error_handling.md
[procmap-proc]: ../processors/README.md#process_map