---
title: parquet_decode
slug: parquet_decode
type: processor
status: experimental
categories: ["Parsing"]
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the corresponding source file under internal/impl/<provider>.
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::caution EXPERIMENTAL
This component is experimental and therefore subject to change or removal outside of major version releases.
:::
Decodes [Parquet files](https://parquet.apache.org/docs/) into a batch of structured messages.

Introduced in version 4.4.0.

```yml
# Config fields, showing default values
label: ""
parquet_decode: {}
```

This processor uses [https://github.com/parquet-go/parquet-go](https://github.com/parquet-go/parquet-go), which is itself experimental. Therefore changes could be made into how this processor functions outside of major version releases.

## Examples

<Tabs defaultValue="Reading Parquet Files from AWS S3" values={[
{ label: 'Reading Parquet Files from AWS S3', value: 'Reading Parquet Files from AWS S3', },
]}>

<TabItem value="Reading Parquet Files from AWS S3">

In this example we consume files from AWS S3 as they're written by listening onto an SQS queue for upload events. We make sure to use the `to_the_end` scanner which means files are read into memory in full, which then allows us to use a `parquet_decode` processor to expand each file into a batch of messages. Finally, we write the data out to local files as newline delimited JSON.

```yaml
input:
  aws_s3:
    bucket: TODO
    prefix: foos/
    scanner:
      to_the_end: {}
    sqs:
      url: TODO
  processors:
    - parquet_decode: {}

output:
  file:
    codec: lines
    path: './foos/${! meta("s3_key") }.jsonl'
```

</TabItem>
</Tabs>

