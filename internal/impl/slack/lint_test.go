// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package slack

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var tokenLintTests = []struct {
	name          string
	componentType string
	conf          string
	lintContains  string // empty means: expect no lint errors
}{
	{
		name:          "slack_post literal token",
		componentType: "output",
		conf: `
slack_post:
  bot_token: "xoxb-not-a-real-token"
  channel_id: "C0123456789"
`,
	},
	{
		name:          "slack_post secret token",
		componentType: "output",
		conf: `
slack_post:
  bot_token: "${secrets.SLACK_APP_BOT_TOKEN}"
  channel_id: "C0123456789"
`,
	},
	{
		name:          "slack_post secret token with literal prefix",
		componentType: "output",
		conf: `
slack_post:
  bot_token: "xoxb-${secrets.SLACK_APP_BOT_TOKEN}"
  channel_id: "C0123456789"
`,
	},
	{
		name:          "slack_post literal token of the wrong type",
		componentType: "output",
		conf: `
slack_post:
  bot_token: "xoxp-not-a-real-token"
  channel_id: "C0123456789"
`,
		lintContains: "field must start with xoxb-",
	},
	{
		name:          "slack_post secret token with a literal prefix of the wrong type",
		componentType: "output",
		conf: `
slack_post:
  bot_token: "xoxp-${secrets.SLACK_APP_BOT_TOKEN}"
  channel_id: "C0123456789"
`,
		lintContains: "field must start with xoxb-",
	},
	{
		name:          "slack secret tokens",
		componentType: "input",
		conf: `
slack:
  app_token: "${secrets.SLACK_APP_TOKEN}"
  bot_token: "${secrets.SLACK_APP_BOT_TOKEN}"
`,
	},
	{
		name:          "slack literal app token of the wrong type",
		componentType: "input",
		conf: `
slack:
  app_token: "xoxb-not-a-real-token"
  bot_token: "xoxb-not-a-real-token"
`,
		lintContains: "field must start with xapp-",
	},
	{
		name:          "slack_users secret token",
		componentType: "input",
		conf: `
slack_users:
  bot_token: "${secrets.SLACK_APP_BOT_TOKEN}"
`,
	},
	{
		name:          "slack_reaction secret token",
		componentType: "output",
		conf: `
slack_reaction:
  bot_token: "${secrets.SLACK_APP_BOT_TOKEN}"
  channel_id: "C0123456789"
  timestamp: "1234567890.123456"
  emoji: "eyes"
`,
	},
	{
		name:          "slack_thread secret token",
		componentType: "processor",
		conf: `
slack_thread:
  bot_token: "${secrets.SLACK_APP_BOT_TOKEN}"
  channel_id: "C0123456789"
  thread_ts: "1234567890.123456"
`,
	},
}

// TestTokenLintRulesUnresolvedSecrets covers linters that leave secret
// references in place because they cannot resolve them.
func TestTokenLintRulesUnresolvedSecrets(t *testing.T) {
	linter := service.GlobalEnvironment().NewComponentConfigLinter()
	linter.SetEnvVarLookupFunc(func(_ context.Context, key string) (string, bool) {
		return "${" + key + "}", true
	})
	runTokenLintTests(t, linter)
}

// TestTokenLintRulesEmptySecrets covers linters that substitute secret
// references they cannot resolve with an empty string.
func TestTokenLintRulesEmptySecrets(t *testing.T) {
	linter := service.GlobalEnvironment().NewComponentConfigLinter()
	linter.SetSkipEnvVarCheck(true)
	linter.SetEnvVarLookupFunc(func(context.Context, string) (string, bool) {
		return "", false
	})
	runTokenLintTests(t, linter)
}

func runTokenLintTests(t *testing.T, linter *service.ComponentConfigLinter) {
	t.Helper()

	for _, test := range tokenLintTests {
		t.Run(test.name, func(t *testing.T) {
			lints, err := linter.LintYAML(test.componentType, []byte(test.conf))
			require.NoError(t, err)

			var combined strings.Builder
			for _, l := range lints {
				fmt.Fprintf(&combined, "%v\n", l)
			}

			if test.lintContains == "" {
				assert.Empty(t, lints, "expected no lint errors, got: %v", combined.String())
				return
			}
			assert.Contains(t, combined.String(), test.lintContains)
		})
	}
}
