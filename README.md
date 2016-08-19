![Benthos](icon.png "Benthos")

Benthos is a low latency stream buffer service for piping large volumes of data
from one source to another, able to protect against back pressure caused by
data surges or service outages further down the pipeline.

Benthos supports a range of input and output protocols out of the box, allowing
you to drop benthos in between many existing pipeline services with minimal
changes to your architecture.

A range of internal buffer strategies are available, allowing you to choose a
balance between latency, protection against back pressure and file based
persistence.

## Design

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

For a full list of available inputs, outputs or buffers you can run:

```
# Print inputs
benthos --print-inputs | less

# Print buffers
benthos --print-buffers | less

# Print outputs
benthos --print-outputs | less
```

## Support

Currently supported input/output targets:

- ZMQ4 (PUSH, PULL, SUB, PUB)
- Nanomsg/Scalability Protocols (PUSH, PULL, SUB, PUB)
- HTTP 1.1 POST
- STDIN/STDOUT
- File
- Kafka 0.9

You can also have multiple outputs or inputs by configuring a routing strategy
(fan in, fan out, round robin, etc).

## Speed and Benchmarks

The main goal of Benthos is to be stable, low-latency and high throughput.
Benthos comes with two benchmarking tools using ZMQ4 `benthos_zmq_producer` and
`benthos_zmq_consumer`. The producer pushes an endless stream of fixed size byte
messages with a configured time interval between each message. The consumer will
read an endless stream of messages and print a running average of latency,
throughput (per second) and total messages received.

These tools are useful for getting ballpark figures, but it would be good to get
some more meaningful third party benchmarks to publish.

Heres a table of results from an 8-core (2.4ghz) machine using the ZMQ4 input,
mmap_file persisted buffer, ZMQ4 output configuration, and with messages of size
5000 bytes:

| Stream Interval | Avg. Latency (us) | Msg. Rate (msgs/s) | Byte Rate (MB/s) |
|----------------:|------------------:|-------------------:|-----------------:|
|           100ms |               247 |               9.66 |             0.05 |
|            10ms |               518 |              75.71 |             0.36 |
|             1ms |               561 |             606.35 |             2.90 |
|           100us |               734 |            4454.05 |            21.32 |
|            10us |              2665 |            7178.02 |            34.36 |
|             1us |            174099 |           20098.26 |            96.20 |

## Install

```shell
go get github.com/jeffail/benthos/cmd/...
```

## ZMQ4 Support

Benthos supports ZMQ4 for both data input and output. To add this you need to
install libzmq4 and use the compile time flag when building benthos:

```shell
go install -tags "ZMQ4" ./cmd/...
```

## Run

```shell
benthos -c ./config.yaml
```

## Config

Create a fully populated default configuration file:

```shell
benthos --print-yaml > config.yaml
benthos --print-json > config.json
```

The configuration file should contain a section for an input, output, and a
buffer. For example, if we wanted to output to a ZMQ4 push socket our output
section in a YAML config might look like this:

```yaml
output:
  type: zmq4
  zmq4:
    addresses:
      - tcp://*:1234
    socket_type: PUSH
```

## More docs

For further information check out the `./docs` directory.

## Vendoring

Versions of go above 1.6 should automatically `go get` all vendored libraries.
Otherwise, while cloning use `--recursive`:

`git clone https://github.com/jeffail/benthos --recursive`

Or, if the repo is already cloned, get the latest libraries with:

`git submodule update --init`

To add new libraries simply run:

```
PACKAGE=github.com/jeffail/util
git submodule add https://$PACKAGE vendor/$PACKAGE"
```

It might be handy to set yourself a function for this in your `.bashrc`:

```
function go-add-vendor {
	git submodule add https://$1 vendor/$1
}
```
