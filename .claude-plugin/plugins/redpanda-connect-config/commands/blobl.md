---
name: rpcn:blobl
description: Create and test Bloblang transformation scripts from natural language descriptions
arguments:
  - name: description
    description: Natural language description of the transformation (e.g., "Convert timestamp to ISO format and uppercase the name field")
    required: true
  - name: sample_input
    description: Optional JSON sample input for testing
    required: false
allowed-tools: ["*"]
---

Use the **rpcn-bloblang-authoring** skill to create a working, tested Bloblang script based on this transformation: **{description}**

If sample_input was provided: **{sample_input}**, use it for testing. Otherwise, create appropriate sample data.

The script must be:
- Tested and validated before presenting
- Include input/output examples
- Handle edge cases (null values, missing fields)
