Benthos Docker
==============

This is a multi stage Dockerfile that builds Benthos and then copies it to a
scratch image. The image comes with a config that allows you to configure simple
bridges using [environment variables](../../config/env/README.md) like this:

``` sh
docker run \
	-e "INPUT_TYPE=kafka_balanced" \
	-e "INPUT_KAFKA_ADDRESSES=foo:6379" \
	-e "OUTPUT_TYPE=nats" \
	-e "OUTPUT_NATS_URLS=nats://bar:4222,nats://baz:4222" \
	benthos
```

Alternatively, you can run the image using a custom config file:

``` sh
docker run --rm -v /path/to/your/config.yaml:/benthos.yaml benthos
```
