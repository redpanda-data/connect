---
name: rpcn:search
description: Search for Redpanda Connect components (inputs, outputs, processors, caches, rate-limits, buffers, metrics, tracers)
arguments:
  - name: component
    description: What component you're looking for (e.g., "kafka consumer", "postgres output", "http server")
    required: true
allowed-tools: ["*"]
---

Use the **component-search** skill to find the right Redpanda Connect components for: **{component}**
