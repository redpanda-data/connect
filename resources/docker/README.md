Benthos Docker
==============

This directory contains two Dockerfile definitions, one is a pure Go image based on [`busybox`][docker.busybox] (`Dockerfile`), the other (`Dockerfile.cgo`) is a CGO enabled build based on [`debian`][docker.debian].

The image has a [default config][default.config] but it's not particularly useful, so you'll either want to use the `-s` cli flag to define config values or copy a config into the path `/benthos.yaml` as a volume.

```shell
# Using a config file
docker run --rm -v /path/to/your/config.yaml:/benthos.yaml ghcr.io/benthosdev/benthos

# Using a series of -s flags
docker run --rm -p 4195:4195 ghcr.io/benthosdev/benthos \
  -s "input.type=http_server" \
  -s "output.type=kafka" \
  -s "output.kafka.addresses=kafka-server:9092" \
  -s "output.kafka.topic=benthos_topic"
```

[docker.busybox]: https://hub.docker.com/_/busybox/
[docker.debian]: https://hub.docker.com/_/debian
[default.config]: ../config/docker.yaml
