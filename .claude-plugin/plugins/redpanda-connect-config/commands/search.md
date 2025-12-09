---
name: rpcn:search
description: Search for Redpanda Connect components (inputs, outputs, processors, caches, rate-limits, buffers, metrics, tracers)
arguments:
  - name: query
    description: Natural language search query (e.g., "kafka consumer", "postgres output", "http server")
    required: true
allowed-tools: ["*"]
---

Use the **rpcn-component-search** skill to help the user find the right Redpanda Connect components for their query: **{query}**

Provide the top 3-5 most relevant components with:
- Component name and category
- Brief description
- Configuration requirements (required/optional fields)
- Minimal usage example
- Why it matches the query
