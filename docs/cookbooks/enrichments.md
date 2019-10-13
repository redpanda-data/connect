Enrichment Workflows
====================

This cookbook demonstrates how to enrich a stream of JSON documents with
HTTP services. This method also works with AWS Lambda functions, subprocesses,
etc.

We will start off by configuring a single enrichment, then we will move onto a
workflow of enrichments with a network of dependencies.

Each enrichment will be performed in parallel across a batch of streamed
documents. Workflow enrichments that do not depend on each other will also be
performed in parallel, making this orchestration method very efficient.

The imaginary problem we are going to solve is applying a set of NLP based
enrichments to a feed of articles in order to detect fake news. We will be
consuming and writing to Kafka, but the example works with any [input][inputs]
and [output][outputs] combination.

Articles are received over the topic `articles` and look like this:

``` json
{
  "type": "article",
  "article": {
    "id": "123foo",
    "title": "Dogs Stop Barking",
    "content": "The world was shocked this morning to find that all dogs have stopped barking."
  }
}
```

## First Enrichment

To start us off we will configure a single enrichment in a pipeline.

### Claims Detector

The first enrichment we are going to add to our pipeline is an imaginary
'claims detector' service. This is an HTTP service that wraps a trained machine
learning model to extract claims that are made within a body of text.

The service expects a `POST` request with JSON payload of the form:

``` json
{
  "text": "The world was shocked this morning to find that all dogs have stopped barking."
}
```

And returns a JSON payload of the form:

``` json
{
  "claims": [
    {
      "entity": "world",
      "claim": "shocked"
    },
    {
      "entity": "dogs",
      "claim": "NOT barking"
    }
  ]
}
```

Since each request only applies to a single document we will make this
enrichment scale by deploying multiple instances and hitting those instances in
parallel across our document batches.

In order to send a mapped request and map the response back into the original
document we will use the [`process_map`][procmap-proc] processor, with a child
[`http`][http-proc] processor.

``` yaml
input:
  kafka_balanced:
    addresses:
    - TODO
    topics:
    - articles
    consumer_group: benthos_articles_group
    batching:
      count: 20 # Tune this to set the size of our document batches.
      period: 1s

pipeline:
  processors:
  - process_map:
      premap:
        text: article.content
      processors:
      - http:
          parallel: true
          request:
            url: http://localhost:4197/claims
            verb: POST
      postmap:
        tmp.claims: claims

output:
  kafka:
    addresses:
    - TODO
    topic: comments_hydrated
```

We configure the [`http`][http-proc] processor to send a batch of documents out
in parallel, but if we were instead using the [`lambda`][lambda-proc] or
[`subprocess`][subproc-proc] processors to hit an enrichment we could wrap them
within the [`parallel`][parallel-proc] processor for the same result.

## Enrichment Workflows

Extracting the claims of an article isn't enough for us to detect fake news, for
that we need to add two more enrichments.

### Hyperbole Detector

Next up is a 'hyperbole detector' that takes a `POST` request containing the
article contents and returns a hyperbole score between 0 and 1. This time the
format is array-based and therefore supports calculating multiple documents
in a single request, making better use of the host machines GPU.

A request should take the following form:

``` json
[
  {
    "text": "The world was shocked this morning to find that all dogs have stopped barking."
  }
]
```

And the response looks like this:

``` json
[
  {
    "hyperbole_rank": 0.73
  }
]
```

In order to create a single request from a batch of documents, and subsequently
map the result back into our batch, we will use the [`archive`][archive-proc]
and [`unarchive`][unarchive-proc] processors in our
[`process_map`][procmap-proc] flow, like this:

``` yaml
  - process_map:
      premap:
        text: article.content
      processors:
      - archive:
          format: json_array
      - http:
          parallel: false
          request:
            url: http://localhost:4198/hyperbole
            verb: POST
      - unarchive:
          format: json_array
      postmap:
        tmp.hyperbole_rank: hyperbole_rank
```

The purpose of the `json_array` format `archive` processor is to take a batch of
JSON documents and place them into a single document as an array. After the
request is made we do the opposite with the `unarchive` processor in order to
convert it back into a batch of the original size.

### Fake News Detector

Finally, we are going to use a 'fake news detector' that takes the article
contents as well as the output of the previous two enrichments and calculates a
fake news rank between 0 and 1.

This service behaves similarly to the claims detector service and takes a
document of the form:

``` json
{
  "text": "The world was shocked this morning to find that all dogs have stopped barking.",
  "hyperbole_rank": 0.73,
  "claims": [
    {
      "entity": "world",
      "claim": "shocked"
    },
    {
      "entity": "dogs",
      "claim": "NOT barking"
    }
  ]
}
```

And returns an object of the form:

``` json
{
  "fake_news_rank": 0.893
}
```

We then wish to map the field `fake_news_rank` from that result into the
original document at the path `article.fake_news_score`. Our
[`process_map`][procmap-proc] block for this enrichment would look like this:

``` yaml
  - process_map:
      premap:
        text: article.content
        claims: tmp.claims
        hyperbole_rank: tmp.hyperbole_rank
      processors:
      - http:
          parallel: true
          request:
            url: http://localhost:4199/fakenews
            verb: POST
      postmap:
        article.fake_news_score: fake_news_rank
```

Note that in our `premap` we are targeting fields that are populated from the
previous two enrichments.

### Combining into a Workflow

Since the dependency graph of our enrichments is simple it would be sufficient
to simply configure these three enrichments sequentially such that the 'fake
news detector' is run last.

However, if we configure our enrichments within a [`process_dag`][procdag-proc]
processor we can use Benthos to automatically detect our dependency graph,
giving us two key benefits:

1. Enrichments at the same level of a dependency graph (claims and hyperbole)
   will be executed in parallel.
2. When introducing more enrichments to our pipeline the added complexity of
   resolving the dependency graph is handled automatically by Benthos.

You can read more about workflows and the advantages of this method
[in this article][workflows].

Using the [`process_dag`][procdag-proc] processor for our enrichments makes our
final pipeline configuration look like this:

``` yaml
input:
  kafka_balanced:
    addresses:
    - TODO
    topics:
    - articles
    consumer_group: benthos_articles_group
    batching:
      count: 20 # Tune this to set the size of our document batches.
      period: 1s

pipeline:
  processors:
  - process_dag:
      claims:
        premap:
          text: article.content
        processors:
        - http:
            parallel: true
            request:
              url: http://localhost:4197/claims
              verb: POST
        postmap:
          tmp.claims: claims

      hyperbole:
        premap:
          text: article.content
        processors:
        - archive:
            format: json_array
        - http:
            parallel: false
            request:
              url: http://localhost:4198/hyperbole
              verb: POST
        - unarchive:
            format: json_array
        postmap:
          tmp.hyperbole_rank: hyperbole_rank

      fake_news:
        premap:
          text: article.content
          claims: tmp.claims
          hyperbole_rank: tmp.hyperbole_rank
        processors:
        - http:
            parallel: true
            request:
              url: http://localhost:4199/fakenews
              verb: POST
        postmap:
          article.fake_news_score: fake_news_rank

  - catch:
    - log:
        fields:
          content: "${!content}"
        message: "Enrichments failed due to: ${!metadata:benthos_processing_failed}"

  - json:
      operator: delete
      path: tmp

output:
  kafka:
    addresses:
    - TODO
    topic: comments_hydrated
```

A [`catch`][catch-proc] processor was added at the end of the pipeline which
catches documents that failed enrichment. You can replace the log event with a
wide range of recovery actions such as sending to a dead-letter/retry queue,
dropping the message entirely, etc. You can read more about error handling
[in this article][error-handling].

[inputs]: ../inputs/README.md
[outputs]: ../outputs/README.md
[error-handling]: ../error_handling.md
[workflows]: ../workflows.md
[catch-proc]: ../processors/README.md#catch
[archive-proc]: ../processors/README.md#archive
[unarchive-proc]: ../processors/README.md#unarchive
[subproc-proc]: ../processors/README.md#subprocessor
[http-proc]: ../processors/README.md#http
[lambda-proc]: ../processors/README.md#lambda
[subprocess-proc]: ../processors/README.md#subprocess
[parallel-proc]: ../processors/README.md#parallel
[procmap-proc]: ../processors/README.md#process_map
[procdag-proc]: ../processors/README.md#process_dag