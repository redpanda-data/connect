---
title: Write a Benthos Plugin
author: Ashley Jeffs
author_url: https://github.com/Jeffail
author_image_url: /img/ash.jpg
description: I made it difficult for our job security
keywords: [
    "benthos",
    "plugin",
    "go",
    "golang",
    "stream processor",
]
tags: [ "Plugins" ]
---

I'm going to walk you through writing a Benthos plugin from scratch in Go.

<!--truncate-->

Too lazy to read? You can find a video equivalent of this post at: [https://youtu.be/Ilah_Y0uMk4](https://youtu.be/Ilah_Y0uMk4). If you prefer to dig straight into code then you should check out the [benthos-plugin-example][plugin-repo] repo.

![benthos-plugged](/img/write-a-benthos-plugin/benthos-plugged.png)

Plugins allow you to embed your code within Benthos as a component. [Processors][benthos-proc] are the most common type of component to get plugged, which is what we're going to do in this post. If you want to run non-Go code from Benthos then you still have options, such as the [`subprocess`][subprocess-proc], [`http`][http-proc] or [`lambda`][lambda-proc] processors.

## Roleplay

Imagine you are a competent engineer. You wrote a function to detect sarcasm in internet posts on a linear scale of 0 to 100:

```go
// HowSarcastic TOTALLY detects sarcasm EVERY time.
func HowSarcastic(content []byte) float64 {
	if bytes.Contains(content, []byte("/s")) {
		return 100
	}
	return 0
}
```

You are confident that `HowSarcastic` is 100% accurate and wish to apply it to a continuous stream of data by deploying it within a stream processing solution.

You want this service to be resilient with at-least-once delivery guarantees, scalable both horizontally and vertically, and able to expose various metrics about the health of the data stream.

You have decided to use Benthos for this service because you love the logo.

![charming-benthos-logo](/img/write-a-benthos-plugin/blobfish.jpg)

### Stuff you don't need to care about yet

Since you are using Benthos you don't need to choose a queue system, metrics aggregator or deployment platform yet, those items can be configured.

You don't even need to know what format the data comes in or how it needs to look when it leaves your service, as Benthos [has plenty of processors][processors] for configuring that stuff on the fly.

## Getting Started

You're going to use Go modules for this one, make a directory and create a `go.mod` file:

```sh
mkdir foo && cd foo
go mod init github.com/bar/foo
```

Next, you need to pull in your only dependency, [Benthos][benthos-repo]:

```sh
go get github.com/Jeffail/benthos/v3
# Look! Now you have more dependencies than friends!
```

That'll automatically add the dep to your `go.mod` file at the latest v3 tag. Next, you're going to write your stream processor service. Write this to the file `main.go`:

```go
package main

import (
    "github.com/Jeffail/benthos/v3/lib/service"
)

func main() {
    service.Run()
}
```

That's it, you've got a full Benthos. If you want to verify then you can run it:

```sh
go run ./main.go --help
```

## Write Your Plugin

Now you will write the actual plugin that executes your function. Processor plugins implement [`types.Processor`][types-processor] and have the signature:

```go
func ProcessMessage(msg types.Message) ([]types.Message, types.Response)
```

A message can have multiple parts (synonymous with a batch) and we are allowed to return either one or more messages or a response which is either an ack or noack.

A message part has both content and any number of metadata key/value pairs. It is therefore up to you as to whether you modify the contents of messages or whether the sarcasm level is added as metadata.

Thankfully you don't need to make that decision now. Instead, you're going to expose it as a config field and support both. The config field will be called `metadata_key`, and if left empty the contents of messages will be replaced entirely with the sarcasm level.

There won't be much code needed so for brevity you are going to write this straight into your `main.go` file:

```go
// SarcasmProc applies our sarcasm detector to messages.
type SarcasmProc struct {
	MetadataKey string `json:"metadata_key" yaml:"metadata_key"`
}

// ProcessMessage returns messages mutated with their sarcasm level.
func (s *SarcasmProc) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	newMsg := msg.Copy()

	newMsg.Iter(func(i int, p types.Part) error {
		sarcasm := HowSarcastic(p.Get())
		sarcasmStr := strconv.FormatFloat(sarcasm, 'f', -1, 64)

		if len(s.MetadataKey) > 0 {
			p.Metadata().Set(s.MetadataKey, sarcasmStr)
		} else {
			p.Set([]byte(sarcasmStr))
		}
		return nil
	})

	return []types.Message{newMsg}, nil
}

// CloseAsync does nothing.
func (s *SarcasmProc) CloseAsync() {}

// WaitForClose does nothing.
func (s *SarcasmProc) WaitForClose(timeout time.Duration) error {
	return nil
}
```

Let's break this down. You have a struct called `SarcasmProc`, which contains a configuration field `MetadataKey`. The functions `CloseAsync` and `WaitForClose` can be ignored as your processor doesn't contain any state that requires termination.

Within your function `ProcessMessage` you iterate all the payloads within the message batch and calculate the sarcasm level with your function `HowSarcastic`. The result is converted into a string and, depending on whether a metadata key has been set, replaces the contents with the result or sets a new metadata value on the payload.

That's your processor completed. Now you need to register the plugin before calling `service.Run`. Since this is a processor plugin you're going to call [`processor.RegisterPlugin`][proc-register-plugin]:

```go
func main() {
	processor.RegisterPlugin(
		"how_sarcastic",
		func() interface{} {
			s := SarcasmProc{}
			return &s
		},
		func(
			iconf interface{},
			mgr types.Manager,
			logger log.Modular,
			stats metrics.Type,
		) (types.Processor, error) {
			return iconf.(*SarcasmProc), nil
		},
	)

	service.Run()
}
```

The first argument is a string that identifies the type of this plugin, that's the string used to specify it within a Benthos config file.

The second argument is a function that creates our config structure, this will be embedded within the Benthos config specification. In this case our processor implementation is the same type as the configuration struct, but you can separate them if you prefer.

The third argument is the generic function that constructs our processor. In this case we've already constructed it as our configuration type and so we can simply cast it and return it.

Now you're going to build your custom Benthos with:

```sh
go build -o benthos
```

## Run Your Plugin

In order to execute your plugin with Benthos you need a config. Write the following to a file `config.yaml`:

```yaml
pipeline:
  processors:
  - type: how_sarcastic
```

And run it:

```sh
./benthos -c ./config.yaml
```

Your config hasn't specified an input or output so they will default to `stdin` and `stdout`. Write the line `'this is not sarcastic'`, followed by the line `'this is sarcastic /s'`. Benthos should print `0` and `100` respectively.

Cool, but this config is pretty useless, good job idiot. Now you're going to fix your mistake. Let's imagine you are processing a stream of JSON documents of the form `{"id":"fooid","content":"this is the content"}` and you want to add a field `sarcasm` containing the sarcasm level of `content`. You can do that purely through config by using the [`json`][json-proc] and [`process_field`][process-field-proc] processors:

```yaml
pipeline:
  processors:
  - json:
      operator: copy
      path: content
      value: sarcasm
  - process_field:
      path: sarcasm
      result_type: float
      processors:
      - type: how_sarcastic
```

Run that config with some JSON documents:

```sh
echo '{"id":"foo1","content":"this is totally sarcastic /s"}
  {"id":"foo2","content":"but this isnt sarcastic at all"}' |
  ./benthos -c ./config.yaml
```

You'll see some log events but also you should see your two modified documents:

```text
{"content":"this is totally sarcastic /s","id":"foo1","sarcasm":100}
{"content":"but this isnt sarcastic at all","id":"foo2","sarcasm":0}
```

That's much more useful, but this is just barely scratching the surface of what Benthos can do. For example, here's a config that calculates sarcasm with your processor and removes anything with a sarcasm level at or above 80:

```yaml
pipeline:
  processors:
  - type: how_sarcastic
    plugin:
      metadata_key: sarcasm
  - filter_parts:
      metadata:
        operator: less_than
        key: sarcasm
        arg: 80
```

Note that it makes use of your `metadata_key` field in order to filter the documents without changing their content.

Try experimenting with other Benthos processors, you can find the documentation at [benthos.dev/docs/components/processors/about][processors].

## Next Steps

After playing around with Benthos processors you should check out the various [inputs][inputs], [outputs][outputs], [metrics aggregators][metrics] and [tracers][tracers] that it's able to hook up with.

For example, here's a modified version of the previous config where we write from Kafka to an S3 bucket, sending our metrics to Prometheus:

```yaml
http:
  address: 0.0.0.0:4195

input:
  kafka:
    addresses:
    - localhost:9092
    consumer_group: foo_consumer_group
    topics:
    - foo_stream

pipeline:
  processors:
  - type: how_sarcastic
    plugin:
      metadata_key: sarcasm
  - filter_parts:
      metadata:
        operator: less_than
        key: sarcasm
        arg: 80

output:
  s3:
    bucket: foo_bucket
    content_type: application/json
    path: ${!metadata:kafka_key}-${!timestamp_unix_nano}-${!count:files}.json

metrics:
  # Endpoint hosted at both :4195/stats and :4195/metrics
  type: prometheus
```

I'm sure you'll make great use of Benthos plugins with your extremely important work /s.

[benthos]: https://www.benthos.dev
[benthos-repo]: https://github.com/Jeffail/benthos
[plugin-repo]: https://github.com/benthosdev/benthos-plugin-example
[processors]: https://benthos.dev/docs/components/processors/about
[inputs]: https://benthos.dev/docs/components/inputs/about
[outputs]: https://benthos.dev/docs/components/outputs/about
[metrics]: https://benthos.dev/docs/components/metrics/about
[tracers]: https://benthos.dev/docs/components/tracers/about
[benthos-proc]: https://benthos.dev/docs/components/processors/about
[json-proc]: https://benthos.dev/docs/components/processors/json
[subprocess-proc]: https://benthos.dev/docs/components/processors/subprocess
[http-proc]: https://benthos.dev/docs/components/processors/http
[lambda-proc]: https://benthos.dev/docs/components/processors/lambda
[process-field-proc]: https://benthos.dev/docs/components/processors/process_field
[types-processor]: https://godoc.org/github.com/Jeffail/benthos/lib/types#Processor
[proc-register-plugin]: https://godoc.org/github.com/Jeffail/benthos/lib/processor#RegisterPlugin
