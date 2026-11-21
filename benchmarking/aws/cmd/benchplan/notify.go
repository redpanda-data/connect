// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/plan"
)

// The Bench Bot agent runs on Redpanda Cloud ADP; its A2A endpoint takes a
// client-credentials token for the production cloud audience.
const notifyAudience = "cloudv2-production.redpanda.cloud"

// Overridable in tests.
var (
	notifyTokenURL   = "https://auth.prd.cloud.redpanda.com/oauth/token"
	notifyHTTPClient = &http.Client{Timeout: 30 * time.Second}
)

// Env the workflow sets from repo vars and secrets.
const (
	envA2AURL       = "BENCH_BOT_A2A_URL"
	envClientID     = "BENCH_BOT_ADP_CLIENT_ID"
	envClientSecret = "BENCH_BOT_ADP_CLIENT_SECRET"
)

var (
	// A run URL is one token with no whitespace, so it cannot add fields to
	// the space-separated message.
	runURLPattern = regexp.MustCompile(`^https://[A-Za-z0-9.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+$`)
	// Catalogue entry ids are bench session ids.
	entryPattern = regexp.MustCompile(`^bench-[0-9]{8}-[0-9]{6}$`)
)

// errNotifySkipped means notify sent nothing because its config is unset
// (exit 5): the plan may still be marked done, unlike a send failure (exit
// 1), which leaves it for a retry.
var errNotifySkipped = errors.New("completion message skipped: notify config unset")

// resultText is the completion message the agent parses.
func resultText(planID, runURL, outcome, entry string) string {
	return fmt.Sprintf("BENCH RESULT plan=%s run=%s outcome=%s entry=%s", planID, runURL, outcome, entry)
}

func runNotify(args []string, stdout io.Writer) error {
	fs := newFlags("notify")
	var planID, runURL, outcome, entry string
	fs.StringVar(&planID, "plan", "", "plan id")
	fs.StringVar(&runURL, "run-url", "", "workflow run URL, or none")
	fs.StringVar(&outcome, "outcome", "", "run outcome")
	fs.StringVar(&entry, "entry", "none", "catalogue entry id, or none")
	if err := parse(fs, args); err != nil {
		return err
	}
	switch {
	case !plan.ValidID(planID):
		return fmt.Errorf("%w: --plan must be a plan id", errUsage)
	case runURL != "none" && !runURLPattern.MatchString(runURL):
		return fmt.Errorf("%w: --run-url must be an https workflow run URL or none", errUsage)
	case !plan.ValidOutcome(outcome):
		return fmt.Errorf("%w: --outcome %q is not a known outcome", errUsage, outcome)
	case entry != "none" && !entryPattern.MatchString(entry):
		return fmt.Errorf("%w: --entry must be a bench session id or none", errUsage)
	}
	text := resultText(planID, runURL, outcome, entry)
	fmt.Fprintln(stdout, text)

	endpoint, id, secret := os.Getenv(envA2AURL), os.Getenv(envClientID), os.Getenv(envClientSecret)
	for _, e := range [][2]string{{envA2AURL, endpoint}, {envClientID, id}, {envClientSecret, secret}} {
		if e[1] == "" {
			fmt.Fprintf(stdout, "::warning::%s is not set; the Bench Bot completion message was not sent\n", e[0])
			return errNotifySkipped
		}
	}
	if u, err := url.Parse(endpoint); err != nil || u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("%s must be an https URL", envA2AURL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := sendA2A(ctx, endpoint, id, secret, text); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "completion message sent")
	return nil
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// sendA2A posts a non-blocking JSON-RPC message/send. The token is minted
// and used here only; neither it nor the secret is ever printed.
func sendA2A(ctx context.Context, endpoint, clientID, clientSecret, text string) error {
	cfg := clientcredentials.Config{
		ClientID:       clientID,
		ClientSecret:   clientSecret,
		TokenURL:       notifyTokenURL,
		EndpointParams: url.Values{"audience": {notifyAudience}},
	}
	client := cfg.Client(context.WithValue(ctx, oauth2.HTTPClient, notifyHTTPClient))

	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      newID(),
		"method":  "message/send",
		"params": map[string]any{
			"configuration": map[string]any{"blocking": false},
			"message": map[string]any{
				"role":      "user",
				"parts":     []any{map[string]any{"kind": "text", "text": text}},
				"messageId": newID(),
			},
		},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building A2A request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		// oauth2 errors can quote the token endpoint's reply, never the
		// token; the url.Error wrapping only names the endpoint.
		return fmt.Errorf("sending completion message: %w", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("completion message: A2A endpoint answered %s", res.Status)
	}
	var rpc struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &rpc); err != nil {
		return fmt.Errorf("completion message: A2A reply is not JSON-RPC: %w", err)
	}
	if rpc.Error != nil {
		return fmt.Errorf("completion message: A2A error %d: %s", rpc.Error.Code, rpc.Error.Message)
	}
	return nil
}
