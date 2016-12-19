![Benthos](icon.png "Benthos")

Benthos is a low latency persistent buffer service for bridging messages, able
to protect against back pressure. A range of messaging protocols are supported,
allowing you to easily drop benthos in between many existing services.

A range of internal buffer strategies are available, allowing you to select a
balance between latency, protection against back pressure and file based
persistence, or nothing at all (direct bridge).

## Design

Benthos has inputs, an optional buffer, and outputs, which are all set in a
single config file.

```
+-------------------------+                  +-------------------------+
|      Input Stream       |                  |     Output Stream       |
| ( ZMQ, HTTP Post, etc ) |--+            +->| ( ZMQ, HTTP Post, etc ) |
+-------------------------+  |            |  +-------------------------+
                             v            |
             +--------------------------------------------+
             |                   Buffer                   |
             | ( Memory-Mapped Files, Memory, None, etc ) |
             +--------------------------------------------+
```

## Supported Protocols

Currently supported input/output targets:

- ZMQ4 (PUSH, PULL, SUB, PUB)
- Nanomsg/Scalability Protocols (PUSH, PULL, SUB, PUB)
- RabbitMQ (AMQP)
- NSQ
- Kafka
- HTTP 1.1 POST/GET
- STDIN/STDOUT
- File

You can also have multiple outputs or inputs by choosing a routing strategy
(fan in, fan out, round robin, etc.)

For a full and up to date list you can print them from the binary:

```
# Print inputs
benthos --print-inputs | less

# Print buffers
benthos --print-buffers | less

# Print outputs
benthos --print-outputs | less
```

## Install

Build with Go:

``` shell
go get github.com/jeffail/benthos/cmd/...
```

Or, [download from here.](https://github.com/Jeffail/benthos/releases)

## Run

``` shell
benthos -c ./config.yaml
```

## Config

Create a fully populated default configuration file:

``` shell
benthos --print-yaml > config.yaml
benthos --print-json > config.json
```

The configuration file should contain a section for an input, output, and a
buffer. For example, if we wanted to output to a ZMQ4 push socket our output
section in a YAML config might look like this:

``` yaml
output:
  type: zmq4
  zmq4:
    addresses:
      - tcp://*:1234
    socket_type: PUSH
```

There are also configuration sections for logging and metrics, if you print an
example config you will see the available options.

## Speed and Benchmarks

Benthos isn't doing much, so it's reasonable to expect low latencies and high
throughput. Here's a table of results from an 8-core (2.4ghz) machine using the
ZMQ4 input, mmap_file persisted buffer, ZMQ4 output configuration, and with
messages of size 5000 bytes:

| Stream Interval | Avg. Latency (us) | Msg. Rate (msgs/s) | Byte Rate (MB/s) |
|----------------:|------------------:|-------------------:|-----------------:|
|           100ms |               247 |               9.66 |             0.05 |
|            10ms |               518 |              75.71 |             0.36 |
|             1ms |               561 |             606.35 |             2.90 |
|           100us |               734 |            4454.05 |            21.32 |
|            10us |              2665 |            7178.02 |            34.36 |
|             1us |            174099 |           20098.26 |            96.20 |

I've added some benchmarking utilities in `./cmd/test`, hopefully a third party
can cook us up some more meaningful figures.

## ZMQ4 Support

Benthos supports ZMQ4 for both data input and output. To add this you need to
install libzmq4 and use the compile time flag when building benthos:

``` shell
go install -tags "ZMQ4" ./cmd/...
```

## Vendoring

Versions of go above 1.6 should automatically `go get` all vendored libraries.
Otherwise, while cloning use `--recursive`:

`git clone https://github.com/jeffail/benthos --recursive`

Or, if the repo is already cloned, get the latest libraries with:

`git submodule update --init`

To add new libraries simply run:

``` shell
PACKAGE=github.com/jeffail/util
git submodule add https://$PACKAGE vendor/$PACKAGE"
```

It might be handy to set yourself a function for this in your `.bashrc`:

``` bash
function go-add-vendor {
	git submodule add https://$1 vendor/$1
}
```

## Docker

There's a `Dockerfile` for creating a benthos docker image. This is built from
scratch and so you'll need to build without CGO (`CGO_ENABLED=0`) for your
benthos build to run within it. Create it like this:

``` shell
CGO_ENABLED=0 make docker
docker run --rm benthos
```

Then use the image:

``` shell
docker run --rm -v ~/benthos.yaml:/config.yaml -v /tmp/data:/data -p 8080:8080 \
	benthos -c /config.yaml
```
