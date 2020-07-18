---
title: Workflows
---

## The Problem

A workflow is often expressed as a [DAG][dag_wiki] of processing stages, where
each stage can result in N possible next stages, until finally the flow ends at
an exit node.

For example, if we had processing stages A, B, C and D, where stage A could
result in either stage B or C being next, always followed by D, it might look
something like this:

```text
.    /--> B --\
A --|          |--> D
     \--> C --/
```

This flow would be easy to express in a standard Benthos config, we could simply
use a [`conditional`][conditional] processor to route to either B or C depending
on a condition on the result of A. However, this method of flow control quickly
becomes unfeasible as the DAG gets more complicated, imagine expressing this
flow using conditional or switch processors:

```text
.     /--> B -------------|--> D
     /                   /
A --|          /--> E --|
     \--> C --|          \
               \----------|--> F
```

And imagine doing so knowing that the diagram is subject to change over time.
Yikes!

## Automatic DAG Resolution

Benthos comes with a workflow solution called the [`process_dag`][process_dag]
processor, which takes a map of [`process_map`][process_map] processors and
calculates the DAG by creating a dependency tree based on their mappings.

This method allows you to build your workflow without having to explicitly
define (or even know) the final order of stages. Instead, you define how each
individual stage mutates the original document and Benthos automatically
resolves the necessary execution order, including parallel execution of stages
within the same dependency tier.

For more information regarding these processors please check their respective
documentation.

### Example

Concepts like these are much easier to explain with a simple example, so let's
define our workflow stages.

We are starting off with arbitrary JSON documents and want to make some HTTP
requests to a series of enrichment services where the end result will be placed
within the original JSON document at the path `tmp.enrichments`.

The enrichment services, depending on the contents of the original payload, will
return content that might need to be fed into a subsequent enrichment service in
order to obtain the final result we are looking for.

Some of these services might fail due to the contents of the original message,
in which case we want to send the full contents to a recovery service that will
attempt to recover a fallback version of the goal enrichment.

The enrichment stages can be described as:

---

A) Perform an HTTP request to `fooserve` with the full message payload as the
contents. The response can be one of the following:

  1. 200: `{"type":"bar","bar":{"some":"fields"}}`
  2. 200: `{"type":"baz","baz":{"some":"fields"}}`
  3. 400: Bad request

---

B) Perform an HTTP request to `barserve` with the object `bar` taken from the
output of stage A response 1. The response can be one of the following:

  1. 200: `{"type":"baz","baz":{"some":"fields"}}`
  2. 400: Bad request

---

C) Perform an HTTP request to `bazserve` with the object `baz` taken either from
the output of stage A response 2 or stage B response 1. The response can be one
of the following:

  1. 200: `{"type":"qux","qux":{"some":"fields"}}`
  2. 400: Bad request

---

D) If any previous stage returns a 400 then we perform an HTTP request to
`recoverserve` with the full message payload as the contents. The response will
always be the following:

  1. 200: `{"type":"qux","qux":{"default":"fields"}}`

---

E) Place the final contents (the object at `qux`) in the document at
`tmp.enrichments.qux`.

---

A diagram for this flow might look like this:

```text
.     /--------|--> C --|----------------|-> E
     /        /          \              /
A --|--> B --|            \            /
     \        \            \          /
      \--------|------------|--> D --|
```

For simplicity we will attempt each HTTP request three times and interpret any
failure after those attempts as the equivalent of a `400` status code.

#### Stage A

There's no need for a premap since we are sending the entire original payload.
The postmap targets are either the object `bar` or `baz`, and both are optional
since we aren't sure which will be present.

```yaml
processors:
  - http:
      parallel: true
      request:
        url: http://fooserve/enrich
        verb: POST
        headers:
          Content-Type: application/json
        backoff_on: [ 429 ]
        drop_on: [ 400 ]
        retries: 3
postmap_optional:
  tmp.enrichments.bar: bar
  tmp.enrichments.baz: baz
```

#### Stage B

The premap for stage B is `tmp.enrichments.bar` which if not present in the
payload results in this stage being skipped. Stage A is the only stage that
provides `tmp.enrichments.bar` and Benthos will make sure stage A is run before
stage B. If we were to add more stages later on that might provide
`tmp.enrichments.bar` then they will also automatically be run before this
stage.

```yaml
premap:
  .: tmp.enrichments.bar
processors:
  - http:
      parallel: true
      request:
        url: http://barserve/enrich
        verb: POST
        headers:
          Content-Type: application/json
        backoff_on: [ 429 ]
        drop_on: [ 400 ]
        retries: 3
postmap:
  tmp.enrichments.baz: baz
```

#### Stage C

The premap for stage C is `tmp.enrichments.baz` which if not present in the
payload results in this stage being skipped. Since either stage A or A might
provide this target they will both be run before this stage.

```yaml
premap:
  .: tmp.enrichments.baz
processors:
  - http:
      parallel: true
      request:
        url: http://bazserve/enrich
        verb: POST
        headers:
          Content-Type: application/json
        backoff_on: [ 429 ]
        drop_on: [ 400 ]
        retries: 3
postmap:
  tmp.enrichments.qux: qux
```

#### Stage D

Stage D is unique since for this stage we need to send the entire contents of
the payload (which means no premap), but we still only wish to trigger this
stage once any other stage capable of providing `tmp.enrichments.qux` has
already been executed. Therefore, we add the field `dependencies` with
`tmp.enrichments.qux` as an explicit dependency.

We also only wish to execute this stage when `tmp.enrichments.qux` is missing
from the payload, therefore we add a condition that performs this check.

```yaml
dependencies:
  - tmp.enrichments.qux
conditions:
  - bloblang: 'this.exists("tmp.enrichments.qux")'
processors:
  - http:
      parallel: true
      request:
        url: http://recoverserve/enrich
        verb: POST
        headers:
          Content-Type: application/json
        backoff_on: [ 429 ]
        drop_on: [ 400 ]
        retries: 3
postmap:
  tmp.enrichments.qux: qux
```

#### Stage E

Stage E doesn't need writing explicitly since by the end of Stage D we should
already have our enrichment at `tmp.enrichments.qux`.

The final Benthos configuration might look something like this:

```yaml
input:
  type: stdin # TODO

pipeline:
  processors:
    - process_dag:
        A:
          processors:
            - http:
                parallel: true
                request:
                  url: http://fooserve/enrich
                  verb: POST
                  headers:
                    Content-Type: application/json
                  backoff_on: [ 429 ]
                  drop_on: [ 400 ]
                  retries: 3
          postmap_optional:
            tmp.enrichments.bar: bar
            tmp.enrichments.baz: baz

        B:
          premap:
            .: tmp.enrichments.bar
          processors:
            - http:
                parallel: true
                request:
                  url: http://barserve/enrich
                  verb: POST
                  headers:
                    Content-Type: application/json
                  backoff_on: [ 429 ]
                  drop_on: [ 400 ]
                  retries: 3
          postmap:
            tmp.enrichments.baz: baz

        C:
          premap:
            .: tmp.enrichments.baz
          processors:
            - http:
                parallel: true
                request:
                  url: http://bazserve/enrich
                  verb: POST
                  headers:
                    Content-Type: application/json
                  backoff_on: [ 429 ]
                  drop_on: [ 400 ]
                  retries: 3
          postmap:
            tmp.enrichments.qux: qux

        D:
          dependencies:
            - tmp.enrichments.qux
          conditions:
            - bloblang: 'this.exists("tmp.enrichments.qux")'
          processors:
            - http:
                parallel: true
                request:
                  url: http://recoverserve/enrich
                  verb: POST
                  headers:
                    Content-Type: application/json
                  backoff_on: [ 429 ]
                  drop_on: [ 400 ]
                  retries: 3
          postmap:
            tmp.enrichments.qux: qux

output:
  type: stdout # TODO
```

## Error Handling

Workflow stages can fail if any mandatory post mappings are unresolvable with
result payloads, or if a processor within the stage fails. When these failures
occur there are many mechanisms within Benthos that allow you to capture and/or
recover from them outlined [in this document][error-handling].

[dag_wiki]: https://en.wikipedia.org/wiki/Directed_acyclic_graph
[conditional]: /docs/components/processors/conditional
[process_dag]: /docs/components/processors/process_dag
[process_map]: /docs/components/processors/process_map
[error-handling]: /docs/configuration/error_handling