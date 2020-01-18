---
title: amqp_0_9
type: input
---

Connects to an AMQP (0.91) queue. AMQP is a messaging protocol used by various
message brokers, including RabbitMQ.


import Tabs from '@theme/Tabs';

<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Advanced', value: 'advanced', },
]}>

import TabItem from '@theme/TabItem';

<TabItem value="common">

```yaml
input:
  amqp_0_9:
    url: amqp://guest:guest@localhost:5672/
    queue: benthos-queue
    consumer_tag: benthos-consumer
    prefetch_count: 10
```

</TabItem>
<TabItem value="advanced">

```yaml
input:
  amqp_0_9:
    url: amqp://guest:guest@localhost:5672/
    queue: benthos-queue
    queue_declare:
      durable: true
      enabled: false
    bindings_declare: []
    consumer_tag: benthos-consumer
    prefetch_count: 10
    prefetch_size: 0
    tls:
      client_certs: []
      enabled: false
      root_cas_file: ""
      skip_cert_verify: false
```

</TabItem>
</Tabs>

TLS is automatic when connecting to an `amqps` URL, but custom
settings can be enabled in the `tls` section.

### Metadata

This input adds the following metadata fields to each message:

``` text
- amqp_content_type
- amqp_content_encoding
- amqp_delivery_mode
- amqp_priority
- amqp_correlation_id
- amqp_reply_to
- amqp_expiration
- amqp_message_id
- amqp_timestamp
- amqp_type
- amqp_user_id
- amqp_app_id
- amqp_consumer_tag
- amqp_delivery_tag
- amqp_redelivered
- amqp_exchange
- amqp_routing_key
- All existing message headers, including nested headers prefixed with the key of their respective parent.
```

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).

## Fields

### `url`

`string` A URL to connect to.

```yaml
# Examples

url: amqp://localhost:5672/

url: amqps://guest:guest@localhost:5672/
```

### `queue`

`string` An AMQP queue to consume from.

### `queue_declare`

`object` Allows you to passively declare the target queue. If the queue already exists
then the declaration passively verifies that they match the target fields.

```yaml
# Examples

queue_declare:
  durable: false
  enabled: true
```

### `bindings_declare`

`array` Allows you to passively declare bindings for the target queue.

```yaml
# Examples

bindings_declare:
- exchange: foo
  key: bar
```

### `consumer_tag`

`string` A consumer tag.

### `prefetch_count`

`number` The maximum number of pending messages to have consumed at a time.

### `prefetch_size`

`number` The maximum amount of pending messages measured in bytes to have consumed at a time.

### `tls`

`object` Custom TLS settings can be used to override system defaults. This includes
providing a collection of root certificate authorities, providing a list of
client certificates to use for client verification and skipping certificate
verification.

Client certificates can either be added by file or by raw contents.

```yaml
# Examples

tls:
  client_certs:
  - cert_file: ./example.pem
    key_file: ./example.key
  enabled: true

tls:
  client_certs:
  - cert: foo
    key: bar
  enabled: true
  skip_cert_verify: true
```


