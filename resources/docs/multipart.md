Multipart Messaging In Benthos
==============================

Benthos natively supports mulitpart messages, which means that multipart
messages can be read, processed, buffered, and written seemlessly. However, some
inputs and outputs do not support multipart.

Inputs that do not support multipart are easy, as they are simply read as
multipart messages with one part. Outputs, however, are more tricky. By default,
an output that only supports single part messages (such as kafka), which
receives a multipart message (perhaps from ZMQ), will output a unique message
per part. These parts are 'at-least once', as message delivery can only be
guaranteed for the whole batch of message parts.

These defaults are not always ideal, sometimes we might want to take a multipart
message, encode it into a binary blob, and output that blob as a single part.
The message could then decoded further in the pipeline by benthos and seemlessly
decoded back into its original multiple part format.

You can do this with the `multi_to_blob` and `blob_to_multi` processors.

## Why

As a quick example, let's consider a platform that has a service `foo`, which
creates and streams messages to a service `bar`. Up until now these services
would connect directly and send multipart messages over ZMQ, looking like this:

```
Foo (ZMQ) => Bar (ZMQ)
```

After some time we decided that we would like to introduce a Kafka cluster so
that messages from `foo` can be replayed independently. In order to save on
development time we decided to use Benthos initially to test the idea before
committing. The pipeline now looks like this:

```
Foo (ZMQ) => Benthos => Kafka => Benthos => Bar (ZMQ)
```

With this config for the first Benthos:

```yaml
TODO
```

And this config for the second Benthos:

```yaml
TODO
```

However, our multipart messages will be written as multiple messages into kafka,
and Bar incorrectly receives single part messages. To fix this we introduce
a `multi_to_blob` processor to the first benthos instance:

```yaml
TODO
```

And a `blob_to_multi` processor to the second instance:

```yaml
TODO
```

Which means we now have multipart messages at both ends.
