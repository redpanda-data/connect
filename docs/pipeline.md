Pipeline
========

Within a Benthos configuration, in between `input` and `output`, is a `pipeline`
section. This section describes an array of [processors][processors] that are to
be applied to *all* messages, and are not bound to any particular input or
output.

If you have processors that are heavy on CPU and aren't specific to a certain
input or output they are best suited for the pipeline section. It is
advantageous to use the pipeline section as it allows you to set an explicit
number of parallel threads of execution which should ideally match the number of
available logical CPU cores.

The following patterns allow you to achieve a distribution of work across these
processing threads for different input arrangments.

### Multiple Consumers

Sometimes our source of data can have many multiple connected clients and will
distribute a stream of messages amongst them. In these circumstances it is
possible to fully utilise a set of parallel processing threads without a buffer,
provided that the number of consumers is greater than the number of threads.
Ideally the number of consumers would be significantly higher than the number of
threads in order to compensate for occasional IO stalls.

For example, imagine we are consuming from a source `baz`, which is
[At-Least-Once][search-alo] and supports multiple connected clients. Our goal is
to read the stream as fast as possible, perform mutations on the JSON payload
using the [jmespath processor][jmespath-processor], and write the resulting
stream to `bar`.

We also wish to take advantage of the delivery guarantees of the source and
therefore want acknowledgements to flow directly from our output sink to the
input source, and therefore need to avoid using a buffer.

For this purpose we would be able to utilise our processing threads without the
need for a buffer. We choose four processing threads to match our 4 CPU cores,
and choose to use eight parallel consumers of the input `baz`.

``` yaml
input:
  type: broker
  broker:
    copies: 8
    inputs:
    - type: baz
buffer:
  type: none
pipeline:
  threads: 4
  processors:
  - type: jmespath
    jmespath:
      query: "reservations[].instances[].[tags[?Key=='Name'].Values[] | [0], type, state.name]"
output:
  type: bar
```

With this config the pipeline within our Benthos instance would look something
like the following:

``` text
baz -\
baz -\
baz ---> processor ---> bar
baz ---> processor -/
baz ---> processor -/
baz ---> processor -/
baz -/
baz -/
```

### Single Consumer

Sometimes a source of data can only have a single consuming client. In these
circumstances it is still possible to have the single stream of data processed
on parallel processing threads by using a [buffer][buffers].

For example, say we have an input stream `foo` with only a single connected
client. Our goal is to read the stream as fast as possible, perform mutations on
the JSON payload using the [jmespath processor][jmespath-processor], and write
the resulting stream to `bar`.

The messages from `foo` are [At-Most-Once][search-amo], and so we are not
concerned with delivery guarantees and want to focus on performance instead. We
have four logical CPU cores on our server and wish to dedicate them all to
processing the data. We believe that the `bar` output will be fast enough to
keep up with the stream with a single connection.

We set our number of processing threads to four in order to match the CPU cores
available. We also chose a `memory` buffer since it is the fastest buffer
option, with a size of 5MB which we have determined to be more than enough to
fit four messages of the stream at any given time.

``` yaml
input:
  type: foo
buffer:
  type: memory
  memory:
    limit: 5000000
pipeline:
  threads: 4
  processors:
  - type: jmespath
    jmespath:
      query: "reservations[].instances[].[tags[?Key=='Name'].Values[] | [0], type, state.name]"
output:
  type: bar
```

With this config the pipeline within our Benthos instance would look something
like the following:

``` text
foo -> memory buffer ---> processor ---> bar
          ( 5MB )    \--> processor -/
                     \--> processor -/
                     \--> processor -/
```

[processors]: ./processors
[jmespath-processor]: ./processors/README.md#jmespath
[buffers]: ./buffers
[search-amo]: https://duckduckgo.com/?q=at+most+once
[search-alo]: https://duckduckgo.com/?q=at+least+once
