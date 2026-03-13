# Telegram Bot Connector

The Telegram connector enables Redpanda Connect to send and receive messages through Telegram bots using the official Bot API.

## Prerequisites

**None!** This is a pure Go implementation with zero external dependencies. No binaries, no CGo, no external services required.

## Quick Start

### 1. Create a Telegram Bot

1. Open Telegram and search for `@BotFather`
2. Send `/newbot` command
3. Follow the prompts to name your bot
4. Copy the bot token (format: `123456789:ABCdefGHIjklMNO...`)
5. (Optional) Customize with `/setdescription`, `/setuserpic`, `/setcommands`

### 2. Get Your Chat ID

To send messages, you need the chat ID of the target chat:

**Method 1: Use the input to discover chat IDs**
```yaml
input:
  telegram:
    bot_token: "${TELEGRAM_BOT_TOKEN}"

output:
  stdout: {}
```
Send a message to your bot, and check the logs for `chat_id`.

**Method 2: Use @userinfobot**
- Search for `@userinfobot` in Telegram
- Send it any message
- It will reply with your user ID

**Method 3: For groups**
- Add your bot to the group
- Send a message in the group
- Check the input logs for the chat ID (will be negative for groups)

## Configuration Examples

### Echo Bot

Receives messages and echoes them back:

```yaml
input:
  telegram:
    bot_token: "${TELEGRAM_BOT_TOKEN}"
    polling_timeout: 30s

output:
  telegram:
    bot_token: "${TELEGRAM_BOT_TOKEN}"
    chat_id: ${! json("message.chat.id") }
    text: "Echo: ${! json("message.text") }"
```

### Notification Bot

Sends alerts to a specific chat:

```yaml
input:
  http_server:
    address: "0.0.0.0:8080"
    path: /alert

pipeline:
  processors:
    - mapping: |
        root.chat_id = env("TELEGRAM_CHAT_ID")
        root.text = "🚨 Alert: " + content().string()

output:
  telegram:
    bot_token: "${TELEGRAM_BOT_TOKEN}"
    chat_id: ${! json("chat_id") }
    text: ${! json("text") }
```

### Monitoring Notifications

Periodic system checks with formatted messages:

```yaml
input:
  generate:
    interval: 5m
    mapping: |
      root.status = "healthy"
      root.timestamp = timestamp_unix()
      root.metrics = {
        "cpu": 45.2,
        "memory": 78.5
      }

pipeline:
  processors:
    - mapping: |
        root.chat_id = env("TELEGRAM_CHAT_ID")
        root.text = """
*System Status Report*

Status: `%s`
Time: `%s`
CPU: `%.1f%%`
Memory: `%.1f%%`
        """.format(
          this.status,
          this.timestamp.ts_format("15:04:05"),
          this.metrics.cpu,
          this.metrics.memory
        )

output:
  telegram:
    bot_token: "${TELEGRAM_BOT_TOKEN}"
    chat_id: ${! json("chat_id") }
    text: ${! json("text") }
    parse_mode: Markdown
```

### Group Admin Bot

Receives commands in a group and responds:

```yaml
input:
  telegram:
    bot_token: "${TELEGRAM_BOT_TOKEN}"

pipeline:
  processors:
    - branch:
        request_map: |
          root = this
          root.is_command = this.message.text.has_prefix("/")
        processors:
          - mapping: |
              root.response = match {
                this.message.text == "/status" => "Bot is running ✅",
                this.message.text == "/help" => "Available commands: /status, /help",
                _ => ""
              }
        result_map: |
          root = this
          root.response_text = this.response

output:
  telegram:
    bot_token: "${TELEGRAM_BOT_TOKEN}"
    chat_id: ${! json("message.chat.id") }
    text: ${! json("response_text") }
```

### Multi-Chat Broadcaster

Send the same message to multiple chats:

```yaml
input:
  stdin: {}

pipeline:
  processors:
    - mapping: |
        root = [
          {"chat_id": "123456789", "text": content()},
          {"chat_id": "987654321", "text": content()},
          {"chat_id": "-1001234567890", "text": content()}
        ]
    - unarchive:
        format: json_array

output:
  telegram:
    bot_token: "${TELEGRAM_BOT_TOKEN}"
    chat_id: ${! json("chat_id") }
    text: ${! json("text") }
```

## Configuration Fields

### Input (`telegram`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bot_token` | string | required | Bot token from @BotFather |
| `polling_timeout` | duration | `30s` | Long polling timeout |
| `allowed_updates` | []string | all | Update types to receive |

**Allowed Update Types:**
- `message` - New messages
- `edited_message` - Edited messages
- `channel_post` - Channel posts
- `edited_channel_post` - Edited channel posts
- `inline_query` - Inline queries
- `chosen_inline_result` - Chosen inline results
- `callback_query` - Callback button presses
- `shipping_query`, `pre_checkout_query`, `poll`, `poll_answer`, `my_chat_member`, `chat_member`, `chat_join_request`

### Output (`telegram`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bot_token` | string | required | Bot token from @BotFather |
| `chat_id` | interpolated string | required | Target chat ID |
| `text` | interpolated string | required | Message text |
| `parse_mode` | string | none | Text formatting mode |
| `disable_notification` | bool | `false` | Silent messages |

**Parse Modes:**

| Mode | Description | Example |
|------|-------------|---------|
| `Markdown` | Markdown formatting | `*bold* _italic_ [link](url)` |
| `MarkdownV2` | Stricter Markdown (requires escaping) | `*bold* _italic_ __underline__` |
| `HTML` | HTML formatting | `<b>bold</b> <i>italic</i> <a href="url">link</a>` |

## Message Structure

Input messages are JSON-serialized Telegram `Update` objects. Common fields:

```json
{
  "update_id": 123456789,
  "message": {
    "message_id": 456,
    "from": {
      "id": 111222333,
      "username": "johndoe",
      "first_name": "John"
    },
    "chat": {
      "id": 111222333,
      "type": "private"
    },
    "date": 1640000000,
    "text": "Hello, bot!"
  }
}
```

**Metadata Fields:**
- `update_id` - Telegram update ID
- `chat_id` - Chat ID (user, group, or channel)
- `user_id` - Sender's user ID
- `message_id` - Message ID
- `message_type` - Type: `message`, `edited_message`, `channel_post`, etc.
- `timestamp` - RFC3339 timestamp

## Rate Limits

Telegram enforces rate limits on bot API calls:

| Limit | Value | Notes |
|-------|-------|-------|
| Global | 30 msg/sec | Default for all bots |
| Per-chat | 1 msg/sec | Per individual chat |
| Groups | 20 msg/min | Per group or channel |
| Paid tier | Up to 1000 msg/sec | 0.1 Stars per message over 30/sec |

**Error Handling:**
- `429 Too Many Requests` - Rate limit exceeded
- Use Redpanda Connect's `rate_limit` resource for high-volume pipelines
- Implement exponential backoff for retries

Example with rate limiting:

```yaml
rate_limit_resources:
  - label: telegram_rate_limit
    local:
      count: 25
      interval: 1s

output:
  telegram:
    bot_token: "${TELEGRAM_BOT_TOKEN}"
    chat_id: ${! json("chat_id") }
    text: ${! json("text") }
    rate_limit: telegram_rate_limit
```

## Troubleshooting

### Invalid Bot Token

**Error:** `failed to validate bot token`

**Solution:**
- Verify token format: `<number>:<hash>` (e.g., `123456789:ABCdef...`)
- Check for typos or extra whitespace
- Ensure token is from @BotFather
- Test manually: `curl https://api.telegram.org/bot<TOKEN>/getMe`

### Chat Not Found

**Error:** `chat_id 123456789 not found (user must start chat with bot first)`

**Solution:**
- User must send `/start` to the bot before receiving messages
- For groups, add the bot to the group first
- Verify chat ID is correct (use input to discover IDs)

### Bot Blocked or Forbidden

**Error:** `bot blocked by user or removed from chat`

**Solution:**
- User has blocked the bot - ask them to unblock it
- Bot was removed from group - re-add the bot
- Check bot permissions in group settings

### Rate Limit Exceeded

**Error:** `rate limit exceeded (max 30 msg/sec, 1 msg/sec per chat)`

**Solution:**
- Reduce message sending rate
- Use `rate_limit` resource in Redpanda Connect
- Consider upgrading to paid tier for higher limits
- Batch notifications when possible

### Network Errors

**Error:** `failed to validate bot token (check token and network)`

**Solution:**
- Check internet connectivity
- Verify firewall allows HTTPS to `api.telegram.org`
- Test with curl: `curl https://api.telegram.org/bot<TOKEN>/getMe`
- Check for proxy requirements

## Best Practices

### Security

- **Never hardcode tokens** - Always use environment variables
- **Rotate tokens periodically** - Use @BotFather's `/token` command
- **Validate input** - Sanitize user messages before processing
- **Use secret management** - Store tokens in HashiCorp Vault, AWS Secrets Manager, etc.

### Performance

- **Use rate limiting** - Prevent API throttling with `rate_limit` resource
- **Batch when possible** - Group notifications to reduce API calls
- **Filter updates** - Use `allowed_updates` to receive only needed update types
- **Monitor errors** - Track 429 errors and adjust send rates

### Reliability

- **Handle errors gracefully** - Implement retry logic with exponential backoff
- **Log chat IDs** - Maintain a registry of active chats
- **Test with real bots** - Create test bots for development
- **Monitor API status** - Check https://t.me/TelegramStatus for outages

### User Experience

- **Format messages** - Use Markdown or HTML for better readability
- **Provide commands** - Use `/setcommands` in @BotFather for command menu
- **Respond quickly** - Acknowledge user messages within seconds
- **Use keyboards** - Implement inline keyboards for interactive bots

## Advanced Features

### Inline Keyboards

Send messages with interactive buttons:

```yaml
# Note: Full inline keyboard support requires custom payload construction
# This is a basic example showing the message structure

pipeline:
  processors:
    - mapping: |
        root.chat_id = this.chat_id
        root.text = "Choose an option:"
        root.reply_markup = {
          "inline_keyboard": [
            [
              {"text": "Option 1", "callback_data": "opt1"},
              {"text": "Option 2", "callback_data": "opt2"}
            ]
          ]
        }
```

### Media Messages

The input receives all message types including photos, documents, voice, and video.
Check the `message` structure for media fields:

```bloblang
# Extract photo file_id
root.file_id = this.message.photo.0.file_id

# Extract document
root.document_id = this.message.document.file_id
root.document_name = this.message.document.file_name
```

## Cloud Compatibility

✅ **Cloud-Safe**: This connector works in all environments:
- Serverless (AWS Lambda, Google Cloud Functions, Azure Functions)
- Containers (Docker, Kubernetes)
- Cloud VMs and managed services
- On-premises deployments

No external binaries or databases required!

## References

- [Telegram Bot API Documentation](https://core.telegram.org/bots/api)
- [BotFather Commands](https://core.telegram.org/bots#6-botfather)
- [Telegram Rate Limits FAQ](https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this)
- [Redpanda Connect Documentation](https://docs.redpanda.com/redpanda-connect/)

## Support

For issues or questions:
- Redpanda Connect: https://github.com/redpanda-data/connect/issues
- Telegram Bot API: https://t.me/BotSupport
