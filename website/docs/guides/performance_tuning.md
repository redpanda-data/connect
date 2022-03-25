---
title: Performance Tuning
---

## Maximising IO Throughput

This section outlines a few common throughput issues and ways in which they can be solved within Benthos.

It is assumed here that your Benthos instance is performing only minor processing steps, and therefore has minimal reliance on your CPU resource. If this is not the case the following still applies to an extent, but you should also refer to [the next section regarding CPU utilisation](#maximising-cpu-utilisation).

Firstly, before venturing into Benthos configurations, you should take an in-depth look at your sources and sinks. Benthos is generally much simpler architecturally than the inputs and outputs it supports. Spend some time understanding how to squeeze the most out of these services and it will make it easier (or unnecessary) to tune your Benthos configuration.

### Benthos Reads Too Slowly

If Benthos isn't reading fast enough from your source it might not necessarily be due to a slow consumer. If the sink is slow this can cause back pressure that throttles the amount Benthos can read. Try consuming a test feed with the output replaced with `drop`. If you notice that the input consumption suddenly speeds up then the issue is likely with the output, in which case [try the next section](#benthos-writes-too-slowly).

If the `drop` output pipe didn't help then take a quick look at the basic configuration fields for the input source type. Sometimes there are fields for setting a number of background prefetches or similar concepts that can increase your throughput. For example, increasing the value of `prefetch_count` for an AMQP consumer can greatly increase the rate at which it is consumed.

Next, if your source supports multiple parallel consumers then you can try doing that within Benthos by using a [broker][broker-input]. For example, if you started with:

```yaml
input:
  http_client:
    url: http://localhost:4195/get
    verb: GET
```

You could change to:

```yaml
input:
  broker:
    copies: 4
    inputs:
      - http_client:
          url: http://localhost:4195/get
          verb: GET
```

Which would create the exact same consumer as before with four connections in total. Try increasing the number of copies to see how that affects the throughput. If your multiple consumers would require different configurations then set copies to `1` and write each consumer as a separate object in the `inputs` array.

Read the [broker documentation][broker-input] for more tips on simplifying broker configs.

If your source doesn't support multiple parallel consumers then unfortunately your options are more limited. A logical next step might be to look at your network/disk configuration to see if that's a potential cause of contention.

### Benthos Writes Too Slowly

If you have an output sink that regularly places back pressure on your source there are a few solutions depending on the details of the issue.

Firstly, you should check the config parameters of your output sink. There are often fields specifically for controlling the level of acknowledgement to expect before moving onto the next message, if these levels of guarantee are overkill you can disable them for greater throughput. For example, setting the `ack_replicas` field to `false` in the Kafka sink can have a high impact on throughput.

If the config parameters for an output sink aren't enough then you can try the following:

#### Increase in flight messages

Most outputs have a field `max_in_flight` that allows you to specify how many messages can be in flight at the same time. Increasing this value can improve throughput significantly.

#### Send messages in batches

Most outputs will send data quicker when messages are batched, this is often done automatically in the background. However, for a few outputs your batches need to be configured. Read the [batching documentation][batching] for more guidance on how to tune message batches within Benthos.

#### Level out input spikes with a buffer

There are many reasons why an input source might have spikes or inconsistent throughput rates. It is possible that your output is capable of keeping up with
the long term average flow of data, but fails to keep up when an intermittent spike occurs.

In situations like these it is sometimes a better use of your hardware and resources to level out the flow of data rather than try and match the peak throughput. This would depend on the frequency and duration of the spikes as well as your latency requirements, and is therefore a matter of judgement.

Leveling out the flow of data can be done within Benthos using a [buffer][buffers]. Buffers allow an input source to store a bounded amount of data temporarily, which a consumer can work through at its own pace. Buffers always have a fixed capacity, which when full will proceed to block the input just like a busy output would.

Therefore, it's still important to have an output that can keep up with the flow of data, the difference that a buffer makes is that the output only needs to keep up with the _average_ flow of data versus the instantaneous flow of data.

For example, if your input usually produces 10 msgs/s, but occasionally spikes to 100 msgs/s, and your output can handle up to 50 msgs/s, it might be possible to configure a buffer large enough to store spikes in their entirety. As long as the average flow of messages from the input remains below 50 msgs/s then your service should be able to continue indefinitely without ever blocking the input source.

## Maximising CPU Utilisation

Some [processors][processors] within Benthos are relatively heavy on your CPU, and can potentially become the bottleneck of a service. In these circumstances it is worth configuring Benthos so that your processors are running on each available core of your machine without contention.

An array of processors in any section of a Benthos config becomes a single logical pipeline of steps running on a single logical thread. The easiest way to create parallel processor threads is to configure them inside the [pipeline][pipeline] configuration block, where we can explicitly set any number of parallel processor threads independent of how many inputs or outputs we want to use.

Please refer [to the documentation regarding pipelines][pipeline] for some examples.

[pipeline]: /docs/configuration/processing_pipelines
[batching]: /docs/configuration/batching
[processors]: /docs/components/processors/about
[buffers]: /docs/components/buffers/about
[broker-input]: /docs/components/inputs/broker
[broker-output]: /docs/components/outputs/broker
