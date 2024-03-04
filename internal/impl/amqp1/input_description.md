### Metadata

This input adds the following metadata fields to each message:

``` text
- amqp_content_type
- amqp_content_encoding
- amqp_creation_time
- All string typed message annotations
```

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

By setting `read_header` to `true`, additional message header properties will be added to each message:

``` text
- amqp_durable
- amqp_priority
- amqp_ttl
- amqp_first_acquirer
- amqp_delivery_count
```

## Performance

This input benefits from receiving multiple messages in flight in parallel for improved performance. 
You can tune the max number of in flight messages with the field `credit`.
