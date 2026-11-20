# Test data

- `audit_event.proto`: the `ocsf.v1.AuditEvent` schema the producer registers
  in the Schema Registry under the topic's `-value` subject, verbatim
  (generated from OCSF 1.8.0 with a read mask, so it is a subset of OCSF).
- `events.jsonl`: `AuditEvent` messages as protojson, one per line as
  `{"name", "key", "event"}`, where `key` is the Kafka record key the
  producer uses (`<ResourceType>/<ResourceID>`).
- `expected.ndjson`: the OCSF 1.8 NDJSON the pipeline must emit for each event,
  same order: proto field names, enums as numbers, `time` as a JSON number,
  out-of-class attributes under `unmapped` (see the package README, "OCSF
  conformance").
- `ocsf-1.8.0.json`: the compiled OCSF 1.8.0 schema from
  `https://schema.ocsf.io/1.8.0/export/v2/schema`, trimmed to the two classes
  the pipeline emits and the objects they reach, with descriptions and
  captions removed. Enums are reduced to their value lists. To refresh,
  download the export and re-apply the same trim.

The events were produced by the producer's own audit-event mapper, the code
that builds what its services write to the topic, over scenarios modelled on
the real emitters: management API calls (get, collection-filtered list, update
with before/after images, denied delete, internal-caller exemption, deny-gate
evaluation error, missing tenant id), LLM proxy (allowed, and blocked by a PII
guardrail), MCP gateway `tools/call`, A2A `message/send`, spending export, and
three direct policy-change events. Identifiers are illustrative; everything
structural is what production emits. `metadata.uid` and
`metadata.correlation_uid` are deterministic UUIDs instead of production's
random ones.

| # | name | key | class_uid | type_uid |
|---|---|---|---|---|
| 1 | management-get-agent-allowed | `Agent/d3kaa1b2c3d4e5f6g7h8` | 6003 | 600302 |
| 2 | management-list-agents-item-1 | `Agent/d3kaa1b2c3d4e5f6g7h8` | 6003 | 600302 |
| 3 | management-list-agents-item-2 | `Agent/d3kaa9z8y7x6w5v4u3t2` | 6003 | 600302 |
| 4 | management-update-agent-entity-change | `Agent/d3kaa1b2c3d4e5f6g7h8` | 3004 | 300403 |
| 5 | management-delete-mcp-server-denied | `McpServer/d3kmm9n8b7v6c5x4z3a2` | 6003 | 600304 |
| 6 | llm-proxy-invoke-allowed | `LLMProvider/d3kll0p9o8i7u6y5t4r3` | 6003 | 600399 |
| 7 | llm-proxy-invoke-guardrail-blocked | `LLMProvider/d3kll0p9o8i7u6y5t4r3` | 6003 | 600399 |
| 8 | mcp-gateway-tools-call-allowed | `McpServerTool/get_customer` | 6003 | 600399 |
| 9 | a2a-proxy-message-send-implicit-deny | `Agent/d3kaa9z8y7x6w5v4u3t2` | 6003 | 600399 |
| 10 | management-exempt-internal-caller | `Trace/environment` | 6003 | 600302 |
| 11 | management-deny-gate-eval-error | `Budget/d3kbb1a2s3d4f5g6h7j8` | 6003 | 600303 |
| 12 | spending-export-allowed | `Spending/environment` | 6003 | 600302 |
| 13 | management-no-tenant-id | `Agent/d3kaa1b2c3d4e5f6g7h8` | 6003 | 600302 |
| 14 | policy-create | `Policy/d3kpp5q6w7e8r9t0y1u2` | 3004 | 300401 |
| 15 | policy-update | `Policy/d3kpp5q6w7e8r9t0y1u2` | 3004 | 300403 |
| 16 | policy-delete | `Policy/d3kpp5q6w7e8r9t0y1u2` | 3004 | 300404 |

When the producer's schema or mapper changes, regenerate all three files on
the producer's side.
