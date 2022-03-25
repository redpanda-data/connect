---
id: enrichments
title: Enrichment Workflows
description: How to configure Benthos to process a workflow of enrichment services.
---

This cookbook demonstrates how to enrich a stream of JSON documents with HTTP services. This method also works with [AWS Lambda functions][processor.lambda], [subprocesses][processor.subprocess], etc.

We will start off by configuring a single enrichment, then we will move onto a workflow of enrichments with a network of dependencies using the [`workflow` processor][processor.workflow].

Each enrichment will be performed in parallel across a [pre-batched][batching] stream of documents. Workflow enrichments that do not depend on each other will also be performed in parallel, making this orchestration method very efficient.

The imaginary problem we are going to solve is applying a set of NLP based enrichments to a feed of articles in order to detect fake news. We will be consuming and writing to Kafka, but the example works with any [input][inputs] and [output][outputs] combination.

Articles are received over the topic `articles` and look like this:

```json
{
  "type": "article",
  "article": {
    "id": "123foo",
    "title": "Dogs Stop Barking",
    "content": "The world was shocked this morning to find that all dogs have stopped barking."
  }
}
```

## Meet the Enrichments

### Claims Detector

To start us off we will configure a single enrichment, which is an imaginary 'claims detector' service. This is an HTTP service that wraps a trained machine learning model to extract claims that are made within a body of text.

The service expects a `POST` request with JSON payload of the form:

```json
{
  "text": "The world was shocked this morning to find that all dogs have stopped barking."
}
```

And returns a JSON payload of the form:

```json
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

Since each request only applies to a single document we will make this enrichment scale by deploying multiple HTTP services and hitting those instances in parallel across our document batches.

In order to send a mapped request and map the response back into the original document we will use the [`branch` processor][processor.branch], with a child [`http`][processor.http] processor.

```yaml
input:
  kafka:
    addresses: [ TODO ]
    topics: [ articles ]
    consumer_group: benthos_articles_group
    batching:
      count: 20 # Tune this to set the size of our document batches.
      period: 1s

pipeline:
  processors:
    - branch:
        request_map: 'root.text = this.article.content'
        processors:
          - http:
              url: http://localhost:4197/claims
              verb: POST
        result_map: 'root.tmp.claims = this.claims'

output:
  kafka:
    addresses: [ TODO ]
    topic: comments_hydrated
```

With this pipeline our documents will come out looking something like this:

```json
{
  "type": "article",
  "article": {
    "id": "123foo",
    "title": "Dogs Stop Barking",
    "content": "The world was shocked this morning to find that all dogs have stopped barking."
  },
  "tmp": {
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
}
```

### Hyperbole Detector

Next up is a 'hyperbole detector' that takes a `POST` request containing the article contents and returns a hyperbole score between 0 and 1. This time the format is array-based and therefore supports calculating multiple documents in a single request, making better use of the host machines GPU.

A request should take the following form:

```json
[
  {
    "text": "The world was shocked this morning to find that all dogs have stopped barking."
  }
]
```

And the response looks like this:

```json
[
  {
    "hyperbole_rank": 0.73
  }
]
```

In order to create a single request from a batch of documents, and subsequently map the result back into our batch, we will use the [`archive`][processor.archive] and [`unarchive`][processor.unarchive] processors in our [`branch`][processor.branch] flow, like this:

```yaml
pipeline:
  processors:
    - branch:
        request_map: 'root.text = this.article.content'
        processors:
          - archive:
              format: json_array
          - http:
              url: http://localhost:4198/hyperbole
              verb: POST
          - unarchive:
              format: json_array
        result_map: 'root.tmp.hyperbole_rank = this.hyperbole_rank'
```

The purpose of the `json_array` format `archive` processor is to take a batch of JSON documents and place them into a single document as an array. Subsequently, we then send one single request for each batch.

After the request is made we do the opposite with the `unarchive` processor in order to convert it back into a batch of the original size.

### Fake News Detector

Finally, we are going to use a 'fake news detector' that takes the article contents as well as the output of the previous two enrichments and calculates a fake news rank between 0 and 1.

This service behaves similarly to the claims detector service and takes a document of the form:

```json
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

```json
{
  "fake_news_rank": 0.893
}
```

We then wish to map the field `fake_news_rank` from that result into the original document at the path `article.fake_news_score`. Our [`branch`][processor.branch] block for this enrichment would look like this:

```yaml
pipeline:
  processors:
    - branch:
        request_map: |
          root.text = this.article.content
          root.claims = this.tmp.claims
          root.hyperbole_rank = this.tmp.hyperbole_rank
        processors:
          - http:
              url: http://localhost:4199/fakenews
              verb: POST
        result_map: 'root.article.fake_news_score = this.fake_news_rank'
```

Note that in our `request_map` we are targeting fields that are populated from the previous two enrichments.

If we were to execute all three enrichments in a sequence we'll end up with a document looking like this:

```json
{
  "type": "article",
  "article": {
    "id": "123foo",
    "title": "Dogs Stop Barking",
    "content": "The world was shocked this morning to find that all dogs have stopped barking.",
    "fake_news_rank": 0.76
  },
  "tmp": {
    "hyperbole_rank": 0.34,
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
}
```

Great! However, as a streaming pipeline this set up isn't ideal as our first two enrichments are independent and could potentially be executed in parallel in order to reduce processing latency.

## Combining into a Workflow

If we configure our enrichments within a [`workflow` processor][processor.workflow] we can use Benthos to automatically detect our dependency graph, giving us two key benefits:

1. Enrichments at the same level of a dependency graph (claims and hyperbole) will be executed in parallel.
2. When introducing more enrichments to our pipeline the added complexity of resolving the dependency graph is handled automatically by Benthos.

Placing our branches within a [`workflow` processor][processor.workflow] makes our final pipeline configuration look like this:

```yaml
input:
  kafka:
    addresses: [ TODO ]
    topics: [ articles ]
    consumer_group: benthos_articles_group
    batching:
      count: 20 # Tune this to set the size of our document batches.
      period: 1s

pipeline:
  processors:
    - workflow:
        meta_path: '' # Don't bother storing branch metadata.
        branches:
          claims:
            request_map: 'root.text = this.article.content'
            processors:
              - http:
                  url: http://localhost:4197/claims
                  verb: POST
            result_map: 'root.tmp.claims = this.claims'

          hyperbole:
            request_map: 'root.text = this.article.content'
            processors:
              - archive:
                  format: json_array
              - http:
                  url: http://localhost:4198/hyperbole
                  verb: POST
              - unarchive:
                  format: json_array
            result_map: 'root.tmp.hyperbole_rank = this.hyperbole_rank'

          fake_news:
            request_map: |
              root.text = this.article.content
              root.claims = this.tmp.claims
              root.hyperbole_rank = this.tmp.hyperbole_rank
            processors:
              - http:
                  url: http://localhost:4199/fakenews
                  verb: POST
            result_map: 'root.article.fake_news_score = this.fake_news_rank'

    - catch:
        - log:
            fields_mapping: 'root.content = content().string()'
            message: "Enrichments failed due to: ${!error()}"

    - bloblang: |
        root = this
        root.tmp = deleted()

output:
  kafka:
    addresses: [ TODO ]
    topic: comments_hydrated
```

Since the contents of `tmp` won't be required downstream we remove it after our enrichments using a [`bloblang` processor][processor.bloblang].

A [`catch`][processor.catch] processor was added at the end of the pipeline which catches documents that failed enrichment. You can replace the log event with a wide range of recovery actions such as sending to a dead-letter/retry queue, dropping the message entirely, etc. You can read more about error handling [in this article][error-handling].

[inputs]: /docs/components/inputs/about
[outputs]: /docs/components/outputs/about
[error-handling]: /docs/configuration/error_handling
[batching]: /docs/configuration/batching
[processor.catch]: /docs/components/processors/catch
[processor.archive]: /docs/components/processors/archive
[processor.unarchive]: /docs/components/processors/unarchive
[processor.bloblang]: /docs/components/processors/bloblang
[processor.subprocess]: /docs/components/processors/subprocess
[processor.lambda]: /docs/components/processors/aws_lambda
[processor.http]: /docs/components/processors/http
[processor.branch]: /docs/components/processors/branch
[processor.workflow]: /docs/components/processors/workflow