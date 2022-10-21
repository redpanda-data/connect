---
title: Processing Pipelines
---

Within a Benthos configuration, in between `input` and `output`, is a `pipeline` section. This section describes an array of [processors][processors] that are to be applied to *all* messages, and are not bound to any particular input or output.

If you have processors that are heavy on CPU and aren't specific to a certain input or output they are best suited for the pipeline section. It is advantageous to use the pipeline section as it allows you to set an explicit number of parallel threads of execution:

```yaml
input:
  resource: foo

pipeline:
  threads: 4
  processors:
    - mapping: |
        root = this
        fans = fans.map_each(match {
          this.obsession > 0.5 => this
          _ => deleted()
        })

output:
  resource: bar
```

If the field `threads` is set to `-1` (the default) it will automatically match the number of logical CPUs available. By default almost all Benthos sources will utilise as many processing threads as have been configured, which makes horizontal scaling easy.

[processors]: /docs/components/processors/about
