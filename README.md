Benthos
=======

Benthos is a service for piping large volumes of data from one source to another with low latency and file based persistence in case of service outages or back pressure.

This is currently an experimental project and should not be used for production systems.

## Design

Benthos uses memory mapped files for storing messages in transit with a low latency impact. If benthos crashes with a buffered queue of messages pending then those messages should be preserved and the buffer is resumed where it left off.

## Install

```shell
go get github.com/jeffail/benthos
```

## ZMQ4

Benthos supports ZMQ4 for both data input and output. To add this you need to install libzmq 4 and use the compile time flag when building benthos:

```shell
go install -tags "ZMQ4"
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
