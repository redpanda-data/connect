---
id: custom-metrics
title: Custom Metrics
description: Learn how to emit custom metrics from messages.
---

You can't build cool graphs without metrics, and [Benthos emits many][internal-metrics]. However, occasionally you might want to also emit custom metrics that track data extracted from messages being processed. In this cookbook we'll explore how to achieve this by configuring Benthos to pull download stats from Github, Dockerhub and Homebrew and emit them as gauges.

## The Basics

Firstly, we need to target an API so let's start with the nice and simple Homebrew API, which we'll poll every 60 seconds.

We can either do it with an [`http_client` input][inputs.http_client] and a [rate limit][rate_limits] that restricts us to one request per 60 seconds, or we can use a [`bloblang` input][inputs.bloblang] to generate a message every 60 seconds that triggers an [`http` processor][processors.http]:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs defaultValue="Processor" values={[
{ label: 'With Processor', value: 'Processor', },
{ label: 'With Input', value: 'Input', },
]}>

<TabItem value="Processor">

```yaml
input:
  bloblang:
    interval: 60s
    mapping: root = ""

pipeline:
  processors:
    - http:
        url: https://formulae.brew.sh/api/formula/benthos.json
        verb: GET
```

</TabItem>

<TabItem value="Input">

```yaml
input:
  http_client:
    url: https://formulae.brew.sh/api/formula/benthos.json
    verb: GET
    rate_limit: brewlimit

resources:
  rate_limits:
    brewlimit:
      local:
        count: 1
        interval: 60s
```

</TabItem>

</Tabs>


For this cookbook we'll continue with the processor option as it makes it easier to deploy it as a [scheduled lambda function][serverless.lambda] later on, which is how I'm currently doing it in real life.

The homebrew formula API gives us a JSON blob that looks like this (removing fields we're not interested in, and with numbers inflated relative to my ego):

```json
{
    "name":"benthos",
    "desc":"Stream processor for mundane tasks written in Go",
    "analytics":{"install":{"30d":{"benthos":78978979},"90d":{"benthos":253339124},"365d":{"benthos":681356871}}}
}
```

This format makes it fairly easy to emit the value of `analytics.install.30d.benthos` as a gauge with the [`metric` processor][processors.metric]:

```yaml
http:
  address: 0.0.0.0:4195

input:
  bloblang:
    interval: 60s
    mapping: root = ""

pipeline:
  processors:
    - http:
        url: https://formulae.brew.sh/api/formula/benthos.json
        verb: GET

    - metric:
        type: gauge 
        name: downloads
        labels:
          source: homebrew
        value: ${! json("analytics.install.30d.benthos") }

    - bloblang: root = deleted()

metrics:
  prometheus:
    prefix: benthos
    path_mapping: if this != "downloads" { deleted() }
```

With the above config we have selected the [`prometheus` metrics type][metrics.prometheus], which allows us to use [Prometheus][prometheus] to scrape metrics from Benthos by polling its HTTP API at the url `http://localhost:4195/stats`.

We have also specified a [`path_mapping`][metrics.prometheus.path_mapping] that deletes any internal metrics usually emitted by Benthos by filtering on our custom metric name.

Finally, there's also a [`bloblang` processor][processors.bloblang] added to the end of our pipeline that deletes all messages since we're not interested in sending the raw data anywhere after this point anyway.

While running this config you can verify that our custom metric is emitted with `curl`:

```sh
curl -s http://localhost:4195/stats | grep downloads
```

Giving something like:

```text
# HELP benthos_downloads Benthos Gauge metric
# TYPE benthos_downloads gauge
benthos_downloads{source="homebrew"} 78978979
```

Easy! The Dockerhub API is also pretty simple, and adding it to our pipeline is just:

<Tabs defaultValue="Diff" values={[
{ label: 'Diff', value: 'Diff', },
{ label: 'Full Config', value: 'Full Config', },
]}>

<TabItem value="Diff">

```diff
           source: homebrew
         value: ${! json("analytics.install.30d.benthos") }
 
+    - bloblang: root = ""
+
+    - http:
+        url: https://hub.docker.com/v2/repositories/jeffail/benthos/
+        verb: GET
+
+    - metric:
+        type: gauge
+        name: downloads
+        labels:
+          source: dockerhub
+        value: ${! json("pull_count") }
+
     - bloblang: root = deleted()
```
</TabItem>

<TabItem value="Full Config">

```yaml
http:
  address: 0.0.0.0:4195

input:
  bloblang:
    interval: 60s
    mapping: root = ""

pipeline:
  processors:
    - http:
        url: https://formulae.brew.sh/api/formula/benthos.json
        verb: GET

    - metric:
        type: gauge 
        name: downloads
        labels:
          source: homebrew
        value: ${! json("analytics.install.30d.benthos") }

    - bloblang: root = ""

    - http:
        url: https://hub.docker.com/v2/repositories/jeffail/benthos/
        verb: GET

    - metric:
        type: gauge
        name: downloads
        labels:
          source: dockerhub
        value: ${! json("pull_count") }

    - bloblang: root = deleted()

metrics:
  prometheus:
    prefix: benthos
    path_mapping: if this != "downloads" { deleted() }
```

</TabItem>

</Tabs>

## Harder Example

So that's the basics covered. Next, we're going to target the Github releases API which gives a slightly more complex payload that looks something like this:

```json
{
  "assets":[
    {"name":"benthos-lambda_X.XX.X_linux_amd64.zip","download_count":543534545},
    {"name":"benthos_X.XX.X_darwin_amd64.tar.gz","download_count":43242342},
    {"name":"benthos_X.XX.X_freebsd_amd64.tar.gz","download_count":534565656},
    {"name":"benthos_X.XX.X_linux_amd64.tar.gz","download_count":743282474324}
  ]
}
```

Where we have an object representing each release asset, of which we want to emit a separate download gauge. In order to do this we're going to use a [`bloblang` processor][processors.bloblang] to remap the payload from Github into an array of objects of the following form:

```json
[
  {"source":"github","dist":"lambda_linux_amd64","download_count":543534545},
  {"source":"github","dist":"darwin_amd64","download_count":43242342},
  {"source":"github","dist":"freebsd_amd64","download_count":534565656},
  {"source":"github","dist":"linux_amd64","download_count":743282474324}
]
```

Then we can use an [`unarchive` processor][processors.unarchive] with the format `json_array` to expand this array into N individual messages, one for each asset. Finally, we will follow up with a [`metric` processor][processors.metric] that dynamically sets labels following the fields `source` and `dist` so that we have a separate metrics series for each asset type.

A simple pipeline of these steps would look like this (please forgive the regexp):

```yaml
http:
  address: 0.0.0.0:4195

input:
  bloblang:
    interval: 60s
    mapping: root = ""

pipeline:
  processors:
    - http:
        url: https://api.github.com/repos/Jeffail/benthos/releases/latest
        verb: GET

    - bloblang: |
        root = this.assets.map_each({
          "source":"github",
          "dist": this.name.re_replace("^benthos-?((lambda_)|_)[0-9\\.]+_([^\\.]+).*", "$2$3"),
          "download_count": this.download_count
        }).filter(this.dist != "checksums")

    - unarchive:
        format: json_array

    - metric:
        type: gauge
        name: downloads
        labels:
          dist: ${! json("dist") }
          source: ${! json("source") }
        value: ${! json("download_count") }

    - bloblang: root = deleted()

metrics:
  prometheus:
    prefix: benthos
    path_mapping: if this != "downloads" { deleted() }
```

Finally, let's combine all the custom metrics into one pipeline.

## Combining into a Workflow

Okay I'm getting bored now so let's wrap this up. The following config expands on the previous examples by configuring each API poll as a [`branch` processor][processors.branch], which allows us to run them within a [`workflow` processor][processors.workflow] that can execute all three branches in parallel.

The [`metric` processors][processors.metric] have also been combined into a single reusable resource by updating the other API calls to format their payloads into the same structure as our Github remap.

```yaml
http:
  address: 0.0.0.0:4195

input:
  bloblang:
    interval: 60s
    mapping: root = {}

pipeline:
  processors:
    - workflow:
        meta_path: results
        order: [ [ dockerhub, github, homebrew ] ]

resources:
  processors:
    dockerhub:
      branch:
        request_map: 'root = ""'
        processors:
          - try:
            - http:
                url: https://hub.docker.com/v2/repositories/jeffail/benthos/
                verb: GET
            - bloblang: |
                root.source = "docker"
                root.dist = "docker"
                root.download_count = this.pull_count
            - resource: metric.gauge

    github:
      branch:
        request_map: 'root = ""'
        processors:
          - try:
            - http:
                url: https://api.github.com/repos/Jeffail/benthos/releases/latest
                verb: GET
            - bloblang: |
                root = this.assets.map_each({
                  "source":"github",
                  "dist": this.name.re_replace("^benthos-?((lambda_)|_)[0-9\\.]+_([^\\.]+).*", "$2$3"),
                  "download_count": this.download_count
                }).filter(this.dist != "checksums")
            - unarchive:
                format: json_array
            - resource: metric.gauge
            - bloblang: 'root = if batch_index() != 0 { deleted() }'

    homebrew:
      branch:
        request_map: 'root = ""'
        processors:
          - try:
            - http:
                url: https://formulae.brew.sh/api/formula/benthos.json
                verb: GET
            - bloblang: |
                root.source = "homebrew"
                root.dist = "homebrew"
                root.download_count = this.analytics.install.30d.benthos
            - resource: metric.gauge

    metric.gauge:
      metric:
        type: gauge
        name: downloads
        labels:
          dist: ${! json("dist") }
          source: ${! json("source") }
        value: ${! json("download_count") }

metrics:
  prometheus:
    prefix: benthos
    path_mapping: if this != "downloads" { deleted() }
```

[serverless.lambda]: /docs/guides/serverless/lambda/
[internal-metrics]: /docs/components/metrics/about/
[inputs.http_client]: /docs/components/inputs/http_client/
[inputs.bloblang]: /docs/components/inputs/bloblang/
[processors.workflow]: /docs/components/processors/workflow/
[processors.branch]: /docs/components/processors/branch/
[processors.unarchive]: /docs/components/processors/unarchive/
[processors.bloblang]: /docs/components/processors/bloblang/
[processors.http]: /docs/components/processors/http/
[processors.metric]: /docs/components/processors/metric/
[rate_limits]: /docs/components/rate_limits/about/
[metrics.prometheus]: /docs/components/metrics/prometheus/
[metrics.prometheus.path_mapping]: /docs/components/metrics/prometheus/#path_mapping
[prometheus]: https://prometheus.io/
