// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SubmitConnector PUTs the given config to /connectors/<name>/config. PUT is
// idempotent (creates or replaces), which is what we want for repeated sweep
// points within a single bench session.
func SubmitConnector(ctx context.Context, baseURL, name string, cfg map[string]any) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	url := fmt.Sprintf("%s/connectors/%s/config", baseURL, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT %s: status %d: %s", url, resp.StatusCode, string(raw))
	}
	return nil
}

type kcConnectorStatus struct {
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
		Trace string `json:"trace,omitempty"`
	} `json:"tasks"`
}

// WaitConnectorRunning polls GET /connectors/<name>/status until both the
// connector and every task report RUNNING, or until ctx is cancelled. If any
// task enters the FAILED state it returns immediately with the trace.
func WaitConnectorRunning(ctx context.Context, baseURL, name string, interval time.Duration) error {
	url := fmt.Sprintf("%s/connectors/%s/status", baseURL, name)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s RUNNING: %w", name, ctx.Err())
		default:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("GET %s: %w", url, err)
		}
		var status kcConnectorStatus
		if resp.StatusCode == http.StatusOK {
			_ = json.NewDecoder(resp.Body).Decode(&status)
		}
		resp.Body.Close()

		for _, t := range status.Tasks {
			if t.State == "FAILED" {
				return fmt.Errorf("connector %s task %d FAILED: %s", name, t.ID, t.Trace)
			}
		}
		if status.Connector.State == "RUNNING" && len(status.Tasks) > 0 {
			allRunning := true
			for _, t := range status.Tasks {
				if t.State != "RUNNING" {
					allRunning = false
					break
				}
			}
			if allRunning {
				return nil
			}
		}
		time.Sleep(interval)
	}
}

// DeleteConnector DELETEs /connectors/<name>. Returns nil for 200, 204, and
// 404 — the latter so callers can use it as an idempotent reset step without
// caring whether the connector already existed.
func DeleteConnector(ctx context.Context, baseURL, name string) error {
	url := fmt.Sprintf("%s/connectors/%s", baseURL, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE %s: %w", url, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE %s: status %d: %s", url, resp.StatusCode, string(raw))
	}
}
