Getting Started
===============

## Install

### Build With Go

If you have Go version 1.11 or above you can build and install Benthos with:

``` sh
go install github.com/Jeffail/benthos/cmd/benthos
benthos -c ./yourconfig.yaml
```

### Pull With Docker

If you have docker installed you can pull the latest official Benthos image
with:

``` sh
docker pull jeffail/benthos
docker run --rm -v /path/to/your/config.yaml:/benthos.yaml jeffail/benthos
```

### Download an Archive

Otherwise you can pull an archive containing Benthos from the
[releases page](https://github.com/Jeffail/benthos/releases).

``` sh
curl -L https://github.com/Jeffail/benthos/releases/download/${VERSION}/benthos_${VERSION}_linux_amd64.tar.gz | tar xz
./benthos -c ./yourconfig.yaml
```

### Serverless

For information about serverless deployments of Benthos check out the serverless
section [here](./serverless/README.md).

## Run

A Benthos stream pipeline is configured with a single config file, you can
generate a fresh one with `benthos --print-yaml` or `benthos --print-json`.

The main sections that make up a pipeline are `input`, `buffer`, `pipeline` and
`output`. If we were to pipe stdin directly to Kafka those sections might look
like this:

``` yaml
input:
  type: stdin
buffer:
  type: none
pipeline:
  threads: 1
  processors: []
output:
  type: kafka
  kafka:
    addresses:
    - localhost:9092
    topic: benthos_stream
```

You can execute this config with:

``` sh
benthos -c ./yourconfig.yaml
```

With this config you can write messages in the terminal and they will be sent to
your chosen Kafka topic, how exciting!

Next, we might want to add some processing steps in order to mutate the
messages. For example, we could add a [`text`](./processors/README.md#text)
processor that converts all text into upper case:

``` yaml
input:
  type: stdin
buffer:
  type: none
pipeline:
  threads: 1
  processors:
  - type: text
    text:
      operator: to_upper
output:
  type: kafka
  kafka:
    addresses:
    - localhost:9092
    topic: benthos_stream
```

Very useful! You can add as many [processing steps](./processors/README.md) as
you like.

Now that you are a Benthos expert check out all the other
[cool stuff you can do](./README.md).