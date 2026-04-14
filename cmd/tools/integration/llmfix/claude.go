// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package llmfix

import (
	_ "embed"
	"encoding/json"
)

var (
	//go:embed triage.md
	triagePrompt string

	//go:embed fix.md
	fixPrompt string
)

// claudeEnvelope is the JSON envelope returned by `claude -p --output-format json`.
type claudeEnvelope struct {
	IsError          bool            `json:"is_error"`
	DurationMS       int             `json:"duration_ms"`
	NumTurns         int             `json:"num_turns"`
	Result           string          `json:"result"`
	StopReason       string          `json:"stop_reason"`
	SessionID        string          `json:"session_id"`
	TotalCostUSD     float64         `json:"total_cost_usd"`
	StructuredOutput json.RawMessage `json:"structured_output"`
	TerminalReason   string          `json:"terminal_reason"`
}

type triageResult struct {
	Issues []issue `json:"issues"`
}

type issue struct {
	Test        string `json:"test"`
	Package     string `json:"package"`
	Type        string `json:"type"`
	Description string `json:"description"`
	JiraKey     string `json:"jira_key,omitempty"`
	IsNew       bool   `json:"is_new"`
}

const triageResultSchema = `{
  "type": "object",
  "required": ["issues"],
  "properties": {
    "issues": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["test", "package", "type", "description"],
        "properties": {
          "test": {"type": "string"},
          "package": {"type": "string"},
          "type": {"enum": ["test_infra", "code_bug"]},
          "description": {"type": "string"},
          "jira_key": {"type": "string"},
          "is_new": {"type": "boolean"}
        }
      }
    }
  }
}`
