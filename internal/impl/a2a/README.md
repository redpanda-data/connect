# A2A (AI-to-AI) Protocol Processor

Redpanda Connect processor for communicating with A2A protocol agents.

## Processor: `a2a_message`

Sends messages to an A2A agent and returns the agent's response.

### Configuration

```yaml
processors:
  - a2a_message:
      agent_card_url: "https://agent.example.com"
      prompt: "${! content() }"  # Optional, defaults to message payload
```

### Environment Variables

**Required** (OAuth2 Client Credentials):
- `REDPANDA_CLOUD_TOKEN_URL` - OAuth2 token endpoint URL
- `REDPANDA_CLOUD_CLIENT_ID` - OAuth2 client ID
- `REDPANDA_CLOUD_CLIENT_SECRET` - OAuth2 client secret

**Optional**:
- `REDPANDA_CLOUD_AUDIENCE` - OAuth2 audience parameter

### Fields

- `agent_card_url` (string, required) - The base URL where the agent card is hosted. The processor fetches the card from `<base_url>/.well-known/agent-card.json` to discover the actual agent endpoint URL.
- `prompt` (string, optional) - Interpolated string for the user prompt. Defaults to message payload.

### Behavior

1. Fetches agent card from `<agent_card_url>/.well-known/agent-card.json` (authenticated with OAuth2)
2. Extracts actual agent endpoint URL from the card
3. Sends a `message/send` request to the A2A agent (with OAuth2 authentication from env vars)
4. If the response is a Task in non-terminal state, polls `tasks/get` every 2 seconds
5. Waits up to 5 minutes for task completion
6. Extracts text from the agent's response
7. Returns response as processor output with metadata

**Note on Authentication**: The processor uses hardcoded OAuth2 client credentials from environment variables. The agent card's `securitySchemes` field is currently ignored.

### Output Metadata

- `a2a_task_id` - The task ID from the A2A agent
- `a2a_context_id` - The context ID for the conversation
- `a2a_status` - The final task status (completed, failed, etc.)

### Example

```yaml
input:
  generate:
    mapping: 'root = "Create a task that gets weather of San Francisco. Output a succinct report."'
    interval: 600s
    count: 1

pipeline:
  processors:
    - a2a_message:
        agent_card_url: "${AGENT_CARD_URL}"
        prompt: "${! content() }"
        final_message_only: true

output:
  processors:
    - log:
        level: INFO
        message: "A2A Response: ${! content() }"
  drop: {}

logger:
  level: INFO
  format: logfmt
```

### Authentication

Authentication uses OAuth2 Client Credentials Grant flow, following the same pattern as other Redpanda Cloud components:

1. Processor reads credentials from environment variables
2. Obtains OAuth2 Bearer token from token endpoint
3. Includes token in all HTTP requests to the agent
4. Token is automatically refreshed as needed

### Protocol Support

- ✅ `message/send` - Send a message (blocking)
- ✅ `tasks/get` - Poll for task completion
- ❌ `message/stream` - Streaming not yet implemented
- ❌ `tasks/resubscribe` - Reconnection not yet implemented

### Error Handling

- Returns error if OAuth2 credentials not configured
- Returns error if agent returns non-text response
- Returns error if task fails or times out
- Logs detailed debug information about requests and responses

## Implementation Details

### Files

- `auth.go` - OAuth2 client credentials helper
- `transport_http.go` - HTTP/JSON-RPC 2.0 transport implementation
- `processor_message.go` - Main processor implementation
- `processor_message_test.go` - Integration tests

### Dependencies

- `github.com/a2aproject/a2a-go` - Official A2A protocol library
- `golang.org/x/oauth2` - OAuth2 client implementation

## References

- [A2A Protocol Specification](https://a2a-protocol.org/latest/specification)
- [a2a-go GitHub Repository](https://github.com/a2aproject/a2a-go)
