# Profiling Tools

This directory contains tools for profiling and monitoring Redpanda Connect performance using Prometheus, Grafana, and pprof.

## Quick Start

1. Start the monitoring stack:
   ```bash
   task up
   ```

2. Run your Redpanda Connect instance with the desired configuration.

3. Access the dashboards:
   - Grafana: http://localhost:3000
   - Prometheus: http://localhost:9090

## Capturing Profiles

In order to use profiling make sure your Redpanda Connect instance has the following configuration: 

```yaml
http:
  debug_endpoints: true
```

Use the following Taskfile commands to capture different types of profiles:

```bash
# Capture all profiles (CPU, memory, blocking)
task profile

# Or capture specific profiles:
task profile:cpu    # 30s CPU profile
task profile:mem    # Memory profile
task profile:block  # Goroutine blocking profile
```

Profiles are saved to the `./profiles` directory, you can use the `pprof` tasks to open them in a browser:

```bash
task pprof:cpu
task pprof:mem
task pprof:block
```

## Cleanup

To stop and remove all containers:

```bash
task down
```
