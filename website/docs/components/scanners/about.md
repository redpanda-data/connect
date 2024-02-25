---
title: Scanners
sidebar_label: About
---

For most Benthos [inputs][input.about] the data consumed comes pre-partitioned into discrete messages which can be comfortably held and processed in memory. However, some inputs such as the [`file` input][input.file] often need to consume data that is large enough that it cannot be processed entirely within memory, and others such as the [`socket` input][input.socket] don't have a concept of consuming the data "entirely".

For such inputs it's necessary to define a mechanism by which the stream of source bytes can be chopped into smaller logical messages, processed and outputted as a continuous process whilst the stream is being read, as this dramatically reduces the memory usage of Benthos as a whole and results in a more fluid flow of data.

The way in which we define this chopping mechanism is through scanners, configured as a field on each input that requires one. For example, if we wished to consume files line-by-line, which each individual line being processed as a discrete message, we could use the [`lines` scanner][scanner.lines] with our [`file` input][input.file]:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Advanced', value: 'advanced', },
]}>

<TabItem value="common">

```yaml
input:
  file:
    paths: [ "./*.txt" ]
    scanner:
      lines: {}
```

</TabItem>
<TabItem value="advanced">

```yaml
# Instead of newlines, use a custom delimiter:
input:
  file:
    paths: [ "./*.txt" ]
    scanner:
      lines:
        custom_delimiter: "---END---"
        max_buffer_size: 100_000_000 # 100MB line buffer
```

</TabItem>
</Tabs>

A scanner is a plugin similar to any other core Benthos component (inputs, processors, outputs, etc), which means it's possible to define your own scanners that can be utilised by inputs that need them.

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="scanners" singular="scanner"></ComponentSelect>

[input.about]: /docs/components/inputs/about
[input.file]: /docs/components/inputs/file
[input.socket]: /docs/components/inputs/socket
[scanner.lines]: /docs/components/scanners/lines
