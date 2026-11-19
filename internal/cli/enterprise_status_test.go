// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/cli"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

func statusTestSchema() *service.ConfigSchema {
	return service.NewEnvironment().
		FullConfigSchema("", "").
		Field(service.NewObjectField("redpanda", enterprise.GlobalRedpandaFields()...))
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func httpGet(addr, path string) (int, []byte, error) {
	res, err := http.Get("http://" + addr + path)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	return res.StatusCode, body, err
}

// runEnterpriseCLI runs InitEnterpriseCLI in the background and returns a
// channel closed when it returns. InitEnterpriseCLI calls os.Exit on a non
// zero exit code, so a failing run aborts the test binary.
func runEnterpriseCLI(args ...string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		cli.InitEnterpriseCLI("meow", "0.0.0", "now", statusTestSchema(), service.CLIOptSetArgs(args...))
	}()
	return done
}

func writeStatusTestConfig(t *testing.T, addr string) string {
	t.Helper()
	confPath := filepath.Join(t.TempDir(), "conf.yaml")
	require.NoError(t, os.WriteFile(confPath, fmt.Appendf(nil, `
http:
  address: %v

input:
  label: inputa
  generate:
    count: 100
    interval: 50ms
    mapping: 'root.id = "foobar"'

pipeline:
  processors:
    - label: shout
      log:
        level: ERROR
        message: 'bad thing at amqp://guest:hunter2@mq:5672'

output:
  label: outputa
  drop: {}
`, addr), 0o644))
	return confPath
}

func waitForCLI(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second * 20):
		t.Fatal("timed out waiting for the CLI to exit")
	}
}

func TestStatusEndpointEnabled(t *testing.T) {
	addr := freeAddr(t)
	confPath := writeStatusTestConfig(t, addr)

	done := runEnterpriseCLI("meow", "run", "--disable-telemetry", "--status-endpoint", confPath)

	type statusLogEntry struct {
		Label     string `json:"label"`
		Path      string `json:"path"`
		Message   string `json:"message"`
		Timestamp int64  `json:"timestamp"`
	}
	type statusBody struct {
		Connection struct {
			Status string           `json:"status"`
			Errors []map[string]any `json:"errors"`
		} `json:"connection"`
		MessagesConsumed      int64            `json:"messages_consumed"`
		LastMessageReceivedAt *int64           `json:"last_message_received_at"`
		RecentErrors          []statusLogEntry `json:"recent_errors"`
	}

	var body statusBody
	require.Eventually(t, func() bool {
		code, raw, err := httpGet(addr, "/status")
		if err != nil || code != http.StatusOK {
			return false
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return false
		}
		if body.Connection.Status != "OK" || body.MessagesConsumed == 0 {
			return false
		}
		if !slices.ContainsFunc(body.RecentErrors, func(e statusLogEntry) bool { return e.Label == "shout" }) {
			return false
		}
		// Listed with the other endpoints. Checked here so it is read while
		// the stream, and with it the HTTP server, is still running.
		code, raw, err = httpGet(addr, "/endpoints")
		if err != nil || code != http.StatusOK {
			return false
		}
		var endpoints map[string]string
		if err := json.Unmarshal(raw, &endpoints); err != nil {
			return false
		}
		_, listed := endpoints["/status"]
		return listed
	}, time.Second*10, time.Millisecond*50, "last body: %+v", body)

	assert.NotNil(t, body.Connection.Errors)
	assert.Empty(t, body.Connection.Errors)

	require.NotNil(t, body.LastMessageReceivedAt)
	assert.InDelta(t, time.Now().Unix(), *body.LastMessageReceivedAt, 30)

	// Other errors may be recorded too (for example a license check at
	// startup), so find the processor's entry by its label.
	var found bool
	for _, entry := range body.RecentErrors {
		if entry.Label != "shout" {
			continue
		}
		found = true
		assert.Equal(t, "pipeline.processors.0", entry.Path)
		assert.Contains(t, entry.Message, "bad thing")
		assert.NotContains(t, entry.Message, "hunter2")
		assert.NotZero(t, entry.Timestamp)
	}
	require.True(t, found, "no recent_errors entry for the shout processor: %+v", body.RecentErrors)

	waitForCLI(t, done)
}

func TestStatusEndpointReportsConnectionError(t *testing.T) {
	addr := freeAddr(t)
	confPath := filepath.Join(t.TempDir(), "conf.yaml")
	require.NoError(t, os.WriteFile(confPath, fmt.Appendf(nil, `
http:
  address: %v

input:
  generate:
    count: 100
    interval: 50ms
    mapping: 'root.id = "foobar"'

output:
  switch:
    cases:
      # Never matches, so no message is ever routed here, but the output still
      # attempts its connection at start up and reports the failure.
      - check: 'false'
        output:
          label: unreachable
          socket:
            network: tcp
            address: 127.0.0.1:1
      - output:
          drop: {}
`, addr), 0o644))

	done := runEnterpriseCLI("meow", "run", "--disable-telemetry", "--status-endpoint", confPath)

	type statusBody struct {
		Connection struct {
			Status string `json:"status"`
			Errors []struct {
				Label   string `json:"label"`
				Path    string `json:"path"`
				Message string `json:"message"`
			} `json:"errors"`
		} `json:"connection"`
	}

	var body statusBody
	require.Eventually(t, func() bool {
		code, raw, err := httpGet(addr, "/status")
		if err != nil || code != http.StatusOK {
			return false
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return false
		}
		return body.Connection.Status == "ERROR"
	}, time.Second*10, time.Millisecond*50, "last body: %+v", body)

	var found bool
	for _, e := range body.Connection.Errors {
		if e.Label != "unreachable" {
			continue
		}
		found = true
		assert.Contains(t, e.Path, "output.switch.cases.0")
		assert.NotEmpty(t, e.Message)
	}
	require.True(t, found, "no connection error for the unreachable output: %+v", body.Connection.Errors)

	waitForCLI(t, done)
}

func TestStatusEndpointDisabledByDefault(t *testing.T) {
	addr := freeAddr(t)
	confPath := writeStatusTestConfig(t, addr)

	done := runEnterpriseCLI("meow", "run", "--disable-telemetry", confPath)

	require.Eventually(t, func() bool {
		code, _, err := httpGet(addr, "/ready")
		return err == nil && code == http.StatusOK
	}, time.Second*10, time.Millisecond*50)

	code, _, err := httpGet(addr, "/status")
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, code)

	waitForCLI(t, done)
}
