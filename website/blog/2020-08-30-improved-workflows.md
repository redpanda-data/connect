---
title: Powered Up Workflows
author: Ashley Jeffs
author_url: https://github.com/Jeffail
author_image_url: /img/ash.jpg
description: Available in v3.26.0
keywords: [
    "benthos",
    "workflows",
    "go",
    "golang",
    "stream processor",
    "enrichments",
]
tags: [ "Workflows" ]
---

For the last few weeks I've been working on improving the workflow story in Benthos. That means reducing the number of processors, simplifying them, and at the same time making them more powerful than before. The new functionality outlined here can be used in the latest release [v3.26.0](https://github.com/Jeffail/benthos/releases/tag/v3.26.0).

<!--truncate-->

## The Motivation

After similar efforts to [improve the mapping story][post.bloblang-beta] in Benthos it seemed sensible to target workflows. Specifically, I've added a new [`branch` processor][processor.branch] for wrapping child processors in request/result maps, and have reworked the [`workflow` processor][processor.workflow] to use them.

If you haven't used workflows in Benthos then there's a section in the new [`workflow` processor][processor.workflow.why] page outlining why they're useful. In short, when performing multiple integrations within a pipeline such as hitting HTTP services, lambdas, caches, etc, it's best to perform them in parallel when possible in order to reduce the processing latency of messages, organizing these integrations into a topology with a workflow makes it easier to manage their interdependencies and ensure they're executed in the right order.

In the old world you could use the `process_dag` processor which has child `process_map` processors, where the mappings were a series of clunky to/from [dot paths][configuration.field_paths], separated into optional and non-optional mappings. There was no way to manually specify the dependency tree, and conditional flows required a separate list of conditions which didn't factor into dependency resolution.

Having such complex and brittle mapping capabilities meant these processors were difficult to document and more so to understand and use.

## Leaning into Bloblang

Thankfully, with [Bloblang][guides.bloblang] now finished it was pretty easy to replace most of the complexity of the workflow mappings for the language itself.

For example, when mapping the request payload for an integration you can express a bunch of different patterns...

Empty request body:

```yaml
request_map: root = ""
```

Sub-object (`foo`) as request body, if the sub-object doesn't exist (or is null) the integration is abandoned:

```yaml
request_map: root = this.foo.not_null()
```

Sub-object as request body which can be obtained from one of a number of possible paths:

```yaml
request_map: root = this.(foo | bar | baz).doc.not_null()
```

Conditional integration applies when the `type` is `foo`, with an unmodified message as request body:

```yaml
request_map: |
  root = if this.type != "foo" {
    deleted()
  }
```

Conditional integration applies when the `type` is `foo`, with a sub-object as the request body:

```yaml
request_map: |
  root = if this.type == "foo" {
    this.foo.not_null()
  } else {
    deleted()
  }
```

Similarly, it's possible to express a bunch of things in the result mapping...

Discard the result (the original message is unchanged):

```yaml
result_map: ""
```

Place the entire result at a path:

```yaml
result_map: root.foo = this
```

Place the result in a metadata field:

```yaml
result_map: meta foo = this
```

If you want to see what it looks like there is an [enrichment cookbook][cookbook.enrichment] that demonstrates workflows in action, but there are also smaller examples on the [workflow page][processor.workflow.examples] such as the following snippet:

```yaml
pipeline:
  processors:
    - workflow:
        meta_path: meta.workflow
        branches:
          foo:
            request_map: 'root = ""'
            processors:
              - http:
                  url: TODO
            result_map: 'root.foo = this'

          bar:
            request_map: 'root = this.body'
            processors:
              - lambda:
                  function: TODO
            result_map: 'root.bar = this'

          baz:
            request_map: |
              root.fooid = this.foo.id
              root.barstuff = this.bar.content
            processors:
              - cache:
                  resource: TODO
                  operator: set
                  key: ${! json("fooid") }
                  value: ${! json("barstuff") }
```

## Conclusion

The docs have been updated to use these new goodies. Obviously the old processors are still being maintained but in a mostly dormant state. The workflow and branch processors are currently labelled as `beta`, but their general behavior is stable with the only exceptions being odd edge cases that might arise.

With the behavior of these processors being dramatically simplified I've also been able to simplify the documentation for them, which also means using more space on the page for example configs.

If you have feedback then [get the absolute heck in the chat you utter recluse][community].

[processor.workflow]: /docs/components/processors/workflow/
[processor.branch]: /docs/components/processors/branch/
[processor.workflow.why]: /docs/components/processors/workflow/#why-use-a-workflow
[processor.workflow.examples]: /docs/components/processors/workflow/#examples
[post.bloblang-beta]: /blog/2020/05/10/bloblang-beta/
[configuration.field_paths]: /docs/configuration/field_paths/
[cookbook.enrichment]: /cookbooks/enrichments/
[guides.bloblang]: /docs/guides/bloblang/about/
[community]: /community/
