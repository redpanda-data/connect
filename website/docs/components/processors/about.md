---
title: Processors
sidebar_label: About
---

Benthos processors are functions applied to messages passing through a pipeline. The function signature allows a processor to mutate or drop messages depending on the content of the message.

Processors are set via config, and depending on where in the config they are placed they will be run either immediately after a specific input (set in the input section), on all messages (set in the pipeline section) or before a specific output (set in the output section). Most processors apply to all messages and can be placed in the pipeline section:

```yaml
pipeline:
  threads: 1
  processors:
    - jmespath:
        query: '{ message: @, meta: { link_count: length(links) } }'
```

The `threads` field in the pipeline section determines how many parallel processing threads are created. You can read more about parallel processing in the [pipeline guide][pipelines].

### Error Handling

Some processors have conditions whereby they might fail. Benthos has mechanisms for detecting and recovering from these failures which can be read about [here][error_handling].

### Batching and Multiple Part Messages

All Benthos processors support multiple part messages, which are synonymous with batches. Some processors such as [split][processor.split] are able to create, expand and break down batches.

Many processors are able to perform their behaviours on specific parts of a message batch, or on all parts, and have a field `parts` for specifying an array of part indexes they should apply to. If the list of target parts is empty these processors will be applied to all message parts.

Part indexes can be negative, and if so the part will be selected from the end counting backwards starting from -1. E.g. if part = -1 then the selected part will be the last part of the message, if part = -2 then the part before the last element will be selected, and so on.

Some processors such as [`filter`][processor.filter] and [`dedupe`][processor.dedupe] act across an entire batch, when instead we'd like to perform them on individual messages of a batch. In this case the [`for_each`][processor.for_each] processor can be used.

[error_handling]: /docs/configuration/error_handling
[pipelines]: /docs/configuration/processing_pipelines
[processor.split]: /docs/components/processors/split
[processor.filter]: /docs/components/processors/filter
[processor.dedupe]: /docs/components/processors/dedupe
[processor.for_each]: /docs/components/processors/for_each
