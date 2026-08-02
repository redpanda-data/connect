// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package slack

import "fmt"

// tokenLintRule builds a lint rule asserting that a Slack token field is
// prefixed with the given token type.
//
// Tokens are usually supplied through a secret or environment variable
// reference such as `${secrets.SLACK_BOT_TOKEN}`. Those references are resolved
// when a config is read, which is not necessarily before it is linted: a linter
// without access to the referenced value either sees the reference verbatim or
// the empty string it falls back to. Neither says anything about the token that
// is used at runtime, so the prefix is only enforced for literal values.
func tokenLintRule(prefix string) string {
	return fmt.Sprintf(
		`root = if this != "" && !this.has_prefix("${") && !this.has_prefix(%q) { [ "field must start with %s" ] }`,
		prefix, prefix,
	)
}
