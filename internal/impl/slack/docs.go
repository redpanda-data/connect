/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package slack

func echobotExample() (string, string, string) {
	return "Echo Slackbot",
		"A slackbot that echo messages from other users", `
input:
  slack:
    app_token: "${APP_TOKEN:xapp-demo}"
    bot_token: "${BOT_TOKEN:xoxb-demo}"
pipeline:
  processors:
    - mutation: |
        # ignore hidden or non message events
        if this.event.type != "message" || (this.event.hidden | false) {
          root = deleted()
        }
        # Don't respond to our own messages
        if this.authorizations.any(auth -> auth.user_id == this.event.user) {
          root = deleted()
        }
output:
  slack_post:
    bot_token: "${BOT_TOKEN:xoxb-demo}"
    channel_id: "${!this.event.channel}"
    thread_ts: "${!this.event.ts}"
    text: "ECHO: ${!this.event.text}"
    `
}
