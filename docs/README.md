<p align="center">
    <a href="https://www.benthos.dev"><img src="./images/logo_dark.svg" style="margin:0;" width="300" alt="Benthos"></a>
</p>

Welcome to the Benthos documentation site, if you haven't used Benthos before check out the [getting started guide](./getting_started.md).

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

## Guides

[Configuration](./configuration.md) explains some config file basics, as well as tools provided by Benthos that make writing and managing them easier.

[Configuration Testing](./configuration_testing.md) explains how to write and execute unit tests for your configuration files.

[Config Interpolation](./config_interpolation.md) explains how to incorporate environment variables and dynamic values into your config files.

[Processing Pipelines](./pipeline.md) explains how processing threads are orchestrated within Benthos and how to fully utilise them.

[Message Batching](./batching.md) explains how multiple part messages and message batching works within Benthos.

[Error Handling](./error_handling.md) explains how you can handle errors from processors in order to recover or reroute the data.

[Performance Tuning](./performance_tuning.md) advises on ways of diagnosing and improving the performance of your Benthos pipelines.

[Monitoring](./monitoring.md) explains how to hook Benthos up to your choice of monitoring and tracing tools.

[Streams Mode](./streams/README.md) outlines a mode of running Benthos where multiple isolated stream pipelines can run in isolation within the same service and be managed using a REST API.

[Workflows](./workflows.md) explains how Benthos can be configured to easily support complex processor flows using automatic DAG resolution.

[Migration](./migration/README.md) outlines migration guides for upgrading to a major version release of Benthos.

## Previous Versions

Previous versions of this documentation can be found for each prior major release at:

- [Version 2](https://docs.benthos.dev/archive/v2/)
- [Version 1](https://docs.benthos.dev/archive/v1/)