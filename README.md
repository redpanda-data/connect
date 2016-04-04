![Benthos](icon.png "Benthos")

Benthos is a stream buffer service for piping large volumes of data from one
source to another using file based persistence. It is a protection against
service outages or back pressure further on in the pipeline caused by surges.

Benthos works well as a bridge between different messaging/stream services as it
is quick to implement and configure new inputs and outputs.

This is currently an experimental project and is not recommended for production
systems.

## Design

Benthos has an input, an optional buffer, and an output, which are set in the
config file.

```
+-------------------------+    +--------+    +-------------------------+
|      Input Streams      |--->| Buffer |--->|     Output Streams      |
| ( ZMQ, HTTP Post, etc ) |    +--------+    | ( ZMQ, HTTP Post, etc ) |
+-------------------------+        |         +-------------------------+
                                   v
               +----------------------------------------+
               | Memory-Mapped Files, Memory, None, etc |
               +----------------------------------------+
```

Benthos supports various input and output strategies. It is possible to combine
multiple inputs and multiple outputs in one service instance. Currently combined
outputs will each receive every message, where any blocked output will block all
outputs. If there is demand then other output paradigms such as publish,
round-robin etc could be supported.

The buffer is where messages can be temporarily stored or persisted across
service restarts depending on your requirements. Memory mapped files are the
recommended approach as this provides low-latency persistence with enough
guarantee for most cases.

## Speed and Benchmarks

Obviously the main goal of Benthos is to be stable, low-latency and high
throughput. Benthos comes with two benchmarking tools using ZMQ4
`benthos_zmq_producer` and `benthos_zmq_consumer`. The producer pushes an
endless stream of fixed size byte messages with a configured time interval
between each message. The consumer will read an endless stream of messages and
print a running average of latency, throughput (per second) and total messages
received.

Using these tools on my laptop shows promising results, but it would be useful
to get some more meaningful third party benchmarks to publish.

A snippet of the results thus far from my laptop using the ZMQ4 input, file
persisted buffer, ZMQ4 output configuration and with messages of size 5000
bytes:

| Stream Interval | Avg. Latency (us) | Msg. Rate (msgs/s) | Byte Rate (MB/s) |
|----------------:|------------------:|-------------------:|-----------------:|
|           100ms |              1196 |               8.31 |             0.04 |
|            10ms |              1529 |              76.87 |             0.37 |
|             1ms |              1148 |             491.62 |             2.35 |
|           100us |              1778 |            1754.60 |             8.40 |
|            10us |             26223 |            2807.33 |            13.44 |

Using a more beefy desktop machine (8 x 2.4ghz cores):

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

Create a default configuration file:

```shell
benthos --print-yaml > config.yaml
benthos --print-json > config.json
```

The configuration file should contain a section for inputs, outputs and a
buffer. The inputs and outputs sections may contain one or more entries, where
each entry will define a unique input or output.

For example, if we wanted to output to both a ZMQ4 push socket as well as
stdout our outputs section in a YAML config might look like this:

```yaml
outputs:
- type: zmq4
  zmq4:
    addresses:
      - tcp://*:1234
    socket_type: PUSH
- type: stdout
```

Or, in a JSON config file it might look like this:

```json
"outputs": [
	{
		"type": "zmq4",
		"zmq4": {
			"addresses": [
				"tcp://*:1235"
			],
			"socket_type": "PUSH"
		}
	},
	{
		"type": "stdout"
	}
]
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
