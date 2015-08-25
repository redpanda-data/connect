Benthos
=======

Benthos is a service for piping large volumes of data from one source to another with low latency and file based persistence in case of service outages or back pressure.

This is currently an experimental project and should not be used for production systems.

## Design

The design foundation of benthos is memory mapped files, these allow us to ensure all incoming traffic is commited to file storage without blocking the stream with disk IO for each individual message. All other design complexity is around supporting multiple writers, readers and topics, and potentially offsets also.

In the event of a service restart benthos will potentially replay previously sent messages.

## Install

```shell
go get github.com/jeffail/benthos
```

## Config

Create a default configuration file:

```shell
benthos --print-yaml > config.yaml
```

## Run

```shell
benthos -conf ./config.yaml
```
