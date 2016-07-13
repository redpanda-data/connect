Inputs
======

Benthos has many input options. This document outlines the input types and
explains any caveats that should be known.

## ZMQ4

ZMQ4 is supported but currently depends on C bindings. Since this is an
annoyance when building or using Benthos it is not compiled by default.

Build it into your project by getting CZMQ installed on your machine, then build
with the tag: `go install -tags "ZMQ4" github.com/jeffail/benthos/cmd/...`

ZMQ4 input supports PULL and SUB sockets only. If there is demand for other
socket types then they can be added easily.

## Scalability Protocols (nanomsg)

The scalability protocols are common communication patterns which will be
familiar to anyone accustomed to service messaging protocols.

This input type should be compatible with any implementation of these protocols,
but nanomsg (http://nanomsg.org/index.html) is the specific target of this type.

Currently only PULL and SUB sockets are supported.

## HTTP

In order to receive messages over HTTP Benthos hosts a server. Messages should
be sent as a POST request. HTTP 1.1 is currently supported and HTTP 2.0 is
planned for the future.

For more information about sending HTTP messages, including details on sending
multipart, please read the `using_http.md` document in this directory.

Websockets are currently not implemented but are simple to add.

## STDIN

The STDIN input simply reads any piped data flowing into the service as line
delimited single part messages. This is a historical input source originally
used for testing. If there is demand then the input could be improved to suit
more cases.

## Setting Multiple Inputs

There are input and output types designed to enable strategies for combining one
or more other input/output options. For inputs your only option here is `fan_in`
where all inputs will be consumed in parallel and combined into a single stream.

In order to configure a `fan_in` type you simply add an array of input
configuration objects into the `inputs` fields, for each of the inputs you wish
to have.

Adding more input types allows you to merge streams from multiple sources into
one. For example, having both a ZMQ4 PULL socket and a Nanomsg PULL socket:

```yaml
type: fan_in
fan_in:
  inputs:
  - type: scalability_protocols
    scalability_protocols:
      address: tcp://nanoserver:3003
      bind_address: false
      socket_type: PULL
  - type: zmq4
    zmq4:
      addresses:
      - tcp://zmqserver:3004
	  socket_type: PULL
```
