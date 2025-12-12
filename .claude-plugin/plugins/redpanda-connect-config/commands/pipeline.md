---
name: rpcn:pipeline
description: Create or repair Redpanda Connect configurations with interactive guidance and validation
arguments:
  - name: target
    description: Either a natural language description (for new configs) or a file path (for fixing existing configs)
    required: true
  - name: context
    description: Optional context when fixing (e.g., "getting connection timeout", "deprecated component")
    required: false
allowed-tools: ["*"]
---

Use the **rpcn-pipeline-assistant** skill to help with: **{target}**

**Two scenarios:**

1. **Creation** (if target is a description):
   - Parse requirements to identify input, output, transformations
   - Discover appropriate components using /rpcn:search
   - Build complete, validated YAML configuration
   - Use environment variables for all secrets
   - Validate with validate-pipeline.sh

2. **Repair** (if target is a file path):
   - Read the configuration at: {target}
   - Diagnose issues (use validation if needed)
   - Consider context: {context}
   - Explain problems clearly
   - Fix minimally, preserving structure
   - Re-validate after changes

**Security:** All passwords, secrets, tokens, API keys must use environment variables (${VAR_NAME}). Create .env.example to document required variables.

**Validation:** Configuration must pass validate-pipeline.sh before completion.
