Documentation
=============

Welcome to the Benthos documentation site, if you haven't used Benthos before
check out the [getting started guide](./getting_started.md).

## Core Components

- [Inputs](./inputs/README.md)
- [Buffers](./buffers/README.md)
- [Pipeline](./pipeline.md)
- [Processors](./processors/README.md)
- [Conditions](./conditions/README.md)
- [Outputs](./outputs/README.md)

## Other Components

- [Caches](./caches/README.md)
- [Rate Limits](./rate_limits/README.md)
- [Metrics](./metrics/README.md)
- [Tracers](./tracers/README.md)

## Other Sections

- [Core Concepts](./concepts.md) describes various Benthos concepts such as:
    - Configuration
    - Monitoring
    - Mutating And Filtering Content
    - Content Based Multiplexing
    - Sharing Resources Across Processors
    - Maximising IO Throughput
    - Maximising CPU Utilisation
- [Message Batching](./batching.md) explains how multiple part messages and
  message batching works within Benthos.
- [Error Handling](./error_handling.md) explains how you can handle errors from
  processor steps in order to recover or reroute the data.
- [Workflows](./workflows.md) explains how Benthos can be configured to easily
  support complex processor flows using automatic DAG resolution.
- [Making Configuration Easier](./configuration.md) explains some of the tools
  provided by Benthos that help make writing configs easier.
- [Config Interpolation](./config_interpolation.md) explains how to incorporate
  environment variables and dynamic values into your config files.
