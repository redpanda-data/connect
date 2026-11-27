// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// Token fixtures are split so secret scanners do not flag the test.
func TestRedactSecretPatterns(t *testing.T) {
	cases := []struct {
		name, in, secret string
	}{
		{"url userinfo upper-case scheme", "POSTGRES://admin:hunter2@db.acme.internal/x", "hunter2"},
		{"url userinfo mixed scheme", "Postgresql+ssl://admin:hunter2@db.acme.internal/x", "hunter2"},
		{"stripe live", "key sk_" + "live_4eC39HqLyjWDarjtT1zdp7dc here", "4eC39HqLyjWDarjtT1zdp7dc"},
		{"stripe test", "SK_" + "TEST_abcDEF123", "abcDEF123"},
		{"github pat classic", "ghp" + "_16C7e42F292c6912E7710c838347Ae178B4a", "16C7e42F292c6912E7710c838347Ae178B4a"},
		{"github oauth", "gho" + "_16C7e42F292c6912E7710c838347Ae178B4a", "16C7e42F292c6912E7710c838347Ae178B4a"},
		{"github fine-grained", "github" + "_pat_11ABCDEFG0_abcdefghijklmnop", "11ABCDEFG0_abcdefghijklmnop"},
		{"google api key", "AI" + "zaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI", "SyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI"},
		{"slack upper-case", "XO" + "XB-1234-abcd", "1234-abcd"},
		{"slack", "xo" + "xp-1234-abcd", "1234-abcd"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := redact(tc.in)
			require.NotContains(t, out, tc.secret)
			require.Contains(t, out, Redacted)
		})
	}
}

func TestRedactKeepsLeftHandSide(t *testing.T) {
	require.Equal(t, "connecting password=REDACTED now", redact("connecting password=hunter22 now"))
	require.Equal(t, "POSTGRES://REDACTED@db.acme.internal/x", redact("POSTGRES://admin:hunter2@db.acme.internal/x"))
}

func TestRedactBloblangOnlyInsideStringLiterals(t *testing.T) {
	cases := []struct{ in, want string }{
		{"root.password = deleted()", "root.password = deleted()"},
		{"root.password = this.password", "root.password = this.password"},
		{"root.token: this.token", "root.token: this.token"},
		{`root.auth = "password=hunter2"`, `root.auth = "password=REDACTED"`},
		{`root.auth = "x password: hunter2 y"`, `root.auth = "x password: REDACTED y"`},
		{`root.a = this.password` + "\n" + `root.b = "token=abc\"def"`, `root.a = this.password` + "\n" + `root.b = "token=REDACTED"`},
		{`root.k = "AKIAIOSFODNN7EXAMPLE"`, `root.k = "REDACTED"`},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.want, redactLogic(tc.in))
		})
	}
}

func TestBuildBloblangAssignmentsSurvive(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{
		map[string]any{"mapping": "root.password = deleted()"},
		map[string]any{"mutation": "root.password = this.password"},
		map[string]any{"bloblang": "root.secret = this.secret"},
		map[string]any{"switch": []any{map[string]any{"check": "this.token == null", "processors": []any{map[string]any{"mapping": `root.auth = "password=hunter2"`}}}}},
	}}
	p := build(t, cfg)
	procs := p.Pipeline["pipeline"].(map[string]any)["processors"].([]any)
	require.Equal(t, "root.password = deleted()", procs[0].(map[string]any)["mapping"])
	require.Equal(t, "root.password = this.password", procs[1].(map[string]any)["mutation"])
	require.Equal(t, "root.secret = this.secret", procs[2].(map[string]any)["bloblang"])
	sw := procs[3].(map[string]any)["switch"].([]any)[0].(map[string]any)
	require.Equal(t, "this.token == null", sw["check"])
	require.Equal(t, `root.auth = "password=REDACTED"`, sw["processors"].([]any)[0].(map[string]any)["mapping"])
	b, err := json.Marshal(p)
	require.NoError(t, err)
	require.NotContains(t, string(b), "hunter2")
}

func TestBuildWorkflowBranchNamesKept(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{map[string]any{"workflow": map[string]any{
		"order": []any{[]any{"auth_lookup", "token_check"}},
		"branches": map[string]any{
			"auth_lookup": map[string]any{
				"request_map": "root = this",
				"processors":  []any{map[string]any{"mapping": "root = this"}},
			},
			"token_check": map[string]any{
				"processors": []any{map[string]any{"mapping": "root = this"}},
			},
		},
	}}}}
	p := build(t, cfg)
	wf := p.Pipeline["pipeline"].(map[string]any)["processors"].([]any)[0].(map[string]any)["workflow"].(map[string]any)
	require.Equal(t, []any{[]any{"auth_lookup", "token_check"}}, wf["order"])
	br := wf["branches"].(map[string]any)
	require.Contains(t, br, "auth_lookup")
	require.Contains(t, br, "token_check")
	require.Equal(t, "root = this", br["auth_lookup"].(map[string]any)["request_map"])
}
