Inputs
======

Benthos has many input options. This document outlines the input types and
explains any caveats that should be known.

## ZMQ4

ZMQ4 is supported but currently depends on C bindings. Since this is an
annoyance when building or using Benthos it is not compiled by default.

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
