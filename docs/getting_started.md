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
[Releases page](https://github.com/Jeffail/benthos/releases).

``` sh
curl -L https://github.com/Jeffail/benthos/releases/download/${VERSION}/benthos_${VERSION}_linux_amd64.tar.gz | tar xz
./benthos -c ./yourconfig.yaml
```

## Run

A Benthos stream pipeline is configured with a single config file made up of
four main sections; input, buffer, pipeline, output. If we were to pipe stdin
directly to Kafka it would look like this:

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

And then point Benthos to your config:

``` sh
benthos -c ./yourconfig.yaml
```

You can write messages in the terminal and they will be sent to your chosen
Kafka topic, how exciting!

Now that you are a Benthos expert the world is your oyster,
[check out all the other cool stuff you can do](./README.md).