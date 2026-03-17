---
name: rpcn:blobl
description: Create and test Bloblang transformation scripts from natural language descriptions
arguments:
  - name: transformation
    description: What transformation you want (e.g., "convert timestamp to ISO format and uppercase name field")
    required: true
  - name: sample
    description: JSON sample input for testing
    required: false
allowed-tools: ["*"]
---

{{#if sample}}
Use the **bloblang-authoring** skill to create a working, tested Bloblang script for: **{transformation}**
Test with this sample input: {sample}
{{else}}
Use the **bloblang-authoring** skill to create a working, tested Bloblang script for: **{transformation}**
{{/if}}
