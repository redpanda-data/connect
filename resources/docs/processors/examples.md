Examples
========

## Sampling Inputs

We have two sources of data that we wish to combine, both from Kafka. One source
is ten times larger than the other, and we wish to sample only 10% that stream
such that both sources are the same proportions in the output stream (ZMQ PUSH).
We can do this with the `sample` processor:

``` yaml
input:
  type: broker
  broker:
    inputs:
    - type: kafka
      kafka:
        addresses:
        - localhost:9092
        topic: benthos_stream_one
    - type: kafka
      kafka:
        addresses:
        - localhost:9092
        topic: benthos_stream_two
      processors:
      - type: sample
        sample:
          retain: 10.0
          seed: 0
output:
  type: zmq4
  zmq4:
    addresses:
    - tcp://*:5556
    bind: true
    socket_type: PUSH
```

With this config our input for the topic `benthos_stream_two` will be randomly
sampled at 10%. Note that we are able to set the random seed, so that we can
deterministically replay the stream later.

## Routing Parts to Outputs

We have an input (ZMQ PULL) that receives messages of two parts and we would
like three different outputs. The first is a file that should only write part
one, the second is ZMQ PUSH that should only write part two, and the third is
ZMQ PUB that should write both. We can do this with the `select_parts`
processor acting as a mutator for the outputs:

``` yaml
input:
  type: zmq4
  zmq4:
    addresses:
    - tcp://localhost:5555
    socket_type: PULL
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: file
      file:
        path: ./part_one.txt
      processors:
      - type: select_parts
        select_parts:
          parts: [ 0 ]
    - type: zmq4
      zmq4:
        addresses:
        - tcp://*:5556
        bind: true
        socket_type: PUSH
      processors:
      - type: select_parts
        select_parts:
          parts: [ 1 ]
    - type: zmq4
      zmq4:
        addresses:
        - tcp://*:5557
        bind: true
        socket_type: PUB
```

## Routing Messages by Number of Parts

This time we have a ZMQ PULL input that receives both single part and multiple
part messages, we want to split these messages into two different ZMQ PUSH
outputs depending on how many parts they have. We can do this with the
`bounds_check` processor to act as a gate keeper for the outputs:

``` yaml
input:
  type: zmq4
  zmq4:
    addresses:
    - tcp://localhost:5555
    socket_type: PULL
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: zmq4
      zmq4:
        addresses:
        - tcp://*:5556
        bind: true
        socket_type: PUSH
      processors:
      - type: bounds_check
        bounds_check:
          min_parts: 2
    - type: zmq4
      zmq4:
        addresses:
        - tcp://*:5557
        bind: true
        socket_type: PUSH
      processors:
      - type: bounds_check
        bounds_check:
          min_parts: 1
          max_parts: 1
```

Using the `bounds_check` processor this way means that the first ZMQ output will
only write messages with at least two parts, the second ZMQ output will only
write messages of exactly one parts.
