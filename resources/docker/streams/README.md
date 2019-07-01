Streams
=======

This docker-compose example shows how to use Benthos in [`--streams`](../../../docs/streams/README.md) mode by setting up a cluster of stream pipelines around NATS.

The cluster consists of following five pipelines which are configured within the `./configs` directory:

- `trickle_samples` reads sample documents from `./sample.json` and continuously writes them to the NATS subject `benthos_messages` at a rate of one message every three seconds.
- `webhooks` creates three webhooks which simulate three customer endpoints, and prints messages received by these webhooks to stdout prefixed with the endpoint that received it.
- `pipe_to_customer_1`, `pipe_to_customer_2` and `pipe_to_customer_3` each read messages from the NATS subject `benthos_messages`, perform an arbitrary filter or mutation to the message and then send the message to its respective target webhook.

It's then possible to monitor how these pipelines interact by observing the stdout pipe of the Benthos instance. You can add, update and remove streams dynamically using the [REST HTTP interface](../../../docs/api/streams.md).

Run
---

```sh
# Run NATS
docker-compose up

# From another shell, run the Benthos pipelines:
benthos --streams --streams-dir ./configs
```
