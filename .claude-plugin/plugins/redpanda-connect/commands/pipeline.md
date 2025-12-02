---
name: rpcn:pipeline
description: Create or repair Redpanda Connect configurations with interactive guidance and validation
arguments:
  - name: context
    description: What you want to build or fix (e.g., "read from kafka and write to postgres", "fix connection timeout error")
    required: true
  - name: file
    description: Path to existing config file to fix or modify
    required: false
allowed-tools: ["*"]
---

{{#if file}}
Use the **pipeline-assistant** skill to help fix or modify the configuration at: **{file}**
Context: {context}
{{else}}
Use the **pipeline-assistant** skill to help create a configuration for: **{context}**
{{/if}}