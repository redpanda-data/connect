# Rate Limiting

**Pattern**: Performance - Rate Limiting
**Difficulty**: Intermediate  
**Components**: rate_limit, http_client
**Use Case**: Control throughput to prevent overwhelming downstream systems

## Overview

Limit request rates to external APIs or services. Prevents rate limit errors and ensures fair resource usage across pipeline instances.

## Configuration

See [`rate-limiting.yaml`](./rate-limiting.yaml)

## Key Concepts

### Local Rate Limiter
- count: Max requests per interval
- interval: Time window

### Resource-Based
Define once, reference everywhere.

## Related

- [Stateful Counter](stateful-counter.md)
