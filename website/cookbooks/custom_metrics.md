---
id: custom-metrics
title: Custom Metrics
description: Learn how to emit custom metrics from messages.
---

You can't build cool graphs without metrics, and [Benthos emits many][internal-metrics]. However, occasionally you might want to also emit custom metrics that track data extracted from messages being processed. In this cookbook we'll explore how to achieve this by configuring Benthos to pull download stats from Github, Dockerhub and Homebrew and emit them as gauges.

## The Basics

Firstly, we need to target an API so let's start with the nice and simple Homebrew API, which we'll poll every 60 seconds.

We can either do it with an [`http_client` input][inputs.http_client] and a [rate limit][rate_limits] that restricts us to one request per 60 seconds, or we can use a [`generate` input][inputs.generate] to generate a message every 60 seconds that triggers an [`http` processor][processors.http]:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs defaultValue="Processor" values={[
{ label: 'With Processor', value: 'Processor', },
{ label: 'With Input', value: 'Input', },
]}>

<TabItem value="Processor">

```yaml
input:
  generate:
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

rate_limit_resources:
  - label: brewlimit
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
  generate:
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
  mapping: if this != "downloads" { deleted() }
  prometheus: {}
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
  generate:
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
  mapping: if this != "downloads" { deleted() }
  prometheus: {}
```

</TabItem>

</Tabs>

## Harder Example

So that's the basics covered. Next, we're going to target the Github releases API which gives a slightly more complex payload that looks something like this:

```json
[
  {
    "tag_name": "X.XX.X",
    "assets":[
      {"name":"benthos-lambda_X.XX.X_linux_amd64.zip","download_count":543534545},
      {"name":"benthos_X.XX.X_darwin_amd64.tar.gz","download_count":43242342},
      {"name":"benthos_X.XX.X_freebsd_amd64.tar.gz","download_count":534565656},
      {"name":"benthos_X.XX.X_linux_amd64.tar.gz","download_count":743282474324}
    ]
  }
]
```

It's an array of objects, one for each tagged release, with a field `assets` which is an array of objects representing each release asset, of which we want to emit a separate download gauge. In order to do this we're going to use a [`bloblang` processor][processors.bloblang] to remap the payload from Github into an array of objects of the following form:

```json
[
  {"source":"github","dist":"lambda_linux_amd64","download_count":543534545,"version":"X.XX.X"},
  {"source":"github","dist":"darwin_amd64","download_count":43242342,"version":"X.XX.X"},
  {"source":"github","dist":"freebsd_amd64","download_count":534565656,"version":"X.XX.X"},
  {"source":"github","dist":"linux_amd64","download_count":743282474324,"version":"X.XX.X"}
]
```

Then we can use an [`unarchive` processor][processors.unarchive] with the format `json_array` to expand this array into N individual messages, one for each asset. Finally, we will follow up with a [`metric` processor][processors.metric] that dynamically sets labels following the fields `source`, `dist` and `version` so that we have a separate metrics series for each asset type for each tagged version.

A simple pipeline of these steps would look like this (please forgive the regexp):

```yaml
http:
  address: 0.0.0.0:4195

input:
  generate:
    interval: 60s
    mapping: root = ""

pipeline:
  processors:
    - http:
        url: https://api.github.com/repos/benthosdev/benthos/releases
        verb: GET

    - bloblang: |
        root = this.map_each(release -> release.assets.map_each(asset -> {
          "source":         "github",
          "dist":           asset.name.re_replace_all("^benthos-?((lambda_)|_)[0-9\\.]+(-rc[0-9]+)?_([^\\.]+).*", "$2$4"),
          "download_count": asset.download_count,
          "version":        release.tag_name.trim("v"),
        }).filter(asset -> asset.dist != "checksums")).flatten()

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
  mapping: if this != "downloads" { deleted() }
  prometheus: {}
```

Finally, let's combine all the custom metrics into one pipeline.

## Combining into a Workflow

Okay I'm getting bored now so let's wrap this up. The following config expands on the previous examples by configuring each API poll as a [`branch` processor][processors.branch], which allows us to run them within a [`workflow` processor][processors.workflow] that can execute all three branches in parallel.

The [`metric` processors][processors.metric] have also been combined into a single reusable resource by updating the other API calls to format their payloads into the same structure as our Github remap.

```yaml
http:
  address: 0.0.0.0:4195

input:
  generate:
    interval: 60s
    mapping: root = {}

pipeline:
  processors:
    - workflow:
        meta_path: results
        order: [ [ dockerhub, github, homebrew ] ]

processor_resources:
  - label: dockerhub
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
              root.version = "all"
          - resource: metric_gauge

  - label: github
    branch:
      request_map: 'root = ""'
      processors:
        - try:
          - http:
              url: https://api.github.com/repos/benthosdev/benthos/releases
              verb: GET
          - bloblang: |
              root = this.map_each(release -> release.assets.map_each(asset -> {
                "source":         "github",
                "dist":           asset.name.re_replace_all("^benthos-?((lambda_)|_)[0-9\\.]+(-rc[0-9]+)?_([^\\.]+).*", "$2$4"),
                "download_count": asset.download_count,
                "version":        release.tag_name.trim("v"),
              }).filter(asset -> asset.dist != "checksums")).flatten()
          - unarchive:
              format: json_array
          - resource: metric_gauge
          - bloblang: 'root = if batch_index() != 0 { deleted() }'

  - label: homebrew
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
              root.version = "all"
          - resource: metric_gauge

  - label: metric_gauge
    metric:
      type: gauge
      name: downloads
      labels:
        dist: ${! json("dist") }
        source: ${! json("source") }
        version: ${! json("version") }
      value: ${! json("download_count") }

metrics:
  mapping: if this != "downloads" { deleted() }
  prometheus: {}
```

[serverless.lambda]: /docs/guides/serverless/lambda
[internal-metrics]: /docs/components/metrics/about
[inputs.http_client]: /docs/components/inputs/http_client
[inputs.generate]: /docs/components/inputs/generate
[processors.workflow]: /docs/components/processors/workflow
[processors.branch]: /docs/components/processors/branch
[processors.unarchive]: /docs/components/processors/unarchive
[processors.bloblang]: /docs/components/processors/bloblang
[processors.http]: /docs/components/processors/http
[processors.metric]: /docs/components/processors/metric
[rate_limits]: /docs/components/rate_limits/about
[metrics.prometheus]: /docs/components/metrics/prometheus
[metrics.about.mapping]: /docs/components/metrics/about#metric-mapping
[prometheus]: https://prometheus.io/
