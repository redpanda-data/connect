// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/gateway"
)

func TestHTTPSinglePayloads(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "0.0.0.0:1234")

	tCtx, done := context.WithTimeout(t.Context(), time.Second*30)
	defer done()

	mux := mux.NewRouter()

	pConf, err := gateway.InputSpec().ParseYAML(`
path: /testpost
`, nil)
	require.NoError(t, err)

	h, err := gateway.InputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, h.RegisterCustomMux(mux))

	server := httptest.NewServer(mux)
	defer server.Close()

	// Test both single and multipart messages.
	for i := range 100 {
		go func() {
			batch, aFn, err := h.ReadBatch(tCtx)
			require.NoError(t, err)

			for _, m := range batch {
				mBytes, err := m.AsBytes()
				require.NoError(t, err)

				m.SetBytes(bytes.ReplaceAll(mBytes, []byte("test"), []byte("response")))
			}

			require.NoError(t, batch.AddSyncResponse())
			require.NoError(t, aFn(tCtx, nil))
		}()

		// Send it as single message
		res, err := http.Post(
			server.URL+"/testpost",
			"application/octet-stream",
			bytes.NewBufferString(fmt.Sprintf("test%v", i)),
		)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		resBytes, err := io.ReadAll(res.Body)
		require.NoError(t, err)

		assert.Equal(t, fmt.Sprintf("response%v", i), string(resBytes))
	}
}

func TestHTTPBatchPayloads(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "0.0.0.0:1234")

	tCtx, done := context.WithTimeout(t.Context(), time.Second*30)
	defer done()

	mux := mux.NewRouter()

	pConf, err := gateway.InputSpec().ParseYAML(`
path: /testpost
`, nil)
	require.NoError(t, err)

	h, err := gateway.InputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, h.RegisterCustomMux(mux))

	server := httptest.NewServer(mux)
	defer server.Close()

	// Test both single and multipart messages.
	for i := range 100 {
		go func() {
			batch, aFn, err := h.ReadBatch(tCtx)
			require.NoError(t, err)

			for _, m := range batch {
				mBytes, err := m.AsBytes()
				require.NoError(t, err)

				m.SetBytes(bytes.ReplaceAll(mBytes, []byte("test"), []byte("response")))
			}

			require.NoError(t, batch.AddSyncResponse())
			require.NoError(t, aFn(tCtx, nil))
		}()

		hdr, body, err := createMultipart([]string{
			fmt.Sprintf("test 0 %v", i),
			fmt.Sprintf("test 1 %v", i),
			fmt.Sprintf("test 2 %v", i),
		}, "application/octet-stream")
		require.NoError(t, err)

		res, err := http.Post(server.URL+"/testpost", hdr, bytes.NewReader(body))
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		act, err := readMultipart(res)
		require.NoError(t, err)
		assert.Equal(t, []string{
			fmt.Sprintf("response 0 %v", i),
			fmt.Sprintf("response 1 %v", i),
			fmt.Sprintf("response 2 %v", i),
		}, act)
	}
}

func createMultipart(payloads []string, contentType string) (hdr string, bodyBytes []byte, err error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for i := 0; i < len(payloads) && err == nil; i++ {
		var part io.Writer
		if part, err = writer.CreatePart(textproto.MIMEHeader{
			"Content-Type": []string{contentType},
		}); err == nil {
			_, err = io.Copy(part, bytes.NewReader([]byte(payloads[i])))
		}
	}

	if err != nil {
		return "", nil, err
	}

	writer.Close()
	return writer.FormDataContentType(), body.Bytes(), nil
}

func readMultipart(res *http.Response) ([]string, error) {
	var params map[string]string
	var err error
	if contentType := res.Header.Get("Content-Type"); contentType != "" {
		if _, params, err = mime.ParseMediaType(contentType); err != nil {
			return nil, err
		}
	}

	var buffer bytes.Buffer
	var output []string

	mr := multipart.NewReader(res.Body, params["boundary"])
	var bufferIndex int64
	for {
		var p *multipart.Part
		if p, err = mr.NextPart(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		var bytesRead int64
		if bytesRead, err = buffer.ReadFrom(p); err != nil {
			return nil, err
		}

		output = append(output, string(buffer.Bytes()[bufferIndex:bufferIndex+bytesRead]))
		bufferIndex += bytesRead
	}

	return output, nil
}

// TestHTTPServerReload tests that the server can be restarted on the same port
// without getting stuck in a "not ready" state. This simulates config reload behavior.
func TestHTTPServerReload(t *testing.T) {
	// Use a random available port
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "127.0.0.1:0")

	tCtx, done := context.WithTimeout(t.Context(), time.Second*30)
	defer done()

	// First server instance
	pConf1, err := gateway.InputSpec().ParseYAML(`
path: /testpost
tcp:
  reuse_port: true
`, nil)
	require.NoError(t, err)

	h1, err := gateway.InputFromParsed(pConf1, service.MockResources())
	require.NoError(t, err)

	// Connect first server (binds to port)
	require.NoError(t, h1.Connect(tCtx))

	// Read handler goroutine for first server
	received1 := make(chan struct{})
	go func() {
		batch, aFn, err := h1.ReadBatch(tCtx)
		if err != nil {
			return
		}
		require.NoError(t, aFn(tCtx, nil))
		require.Len(t, batch, 1)
		close(received1)
	}()

	// Give server time to start listening
	time.Sleep(100 * time.Millisecond)

	// Get the actual bound address from the first server
	// Since we used port 0, we need to extract the actual port
	// For this test, we'll use a fixed port instead
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "127.0.0.1:19283")

	// Recreate with fixed port
	h1.Close(tCtx)

	pConf1, err = gateway.InputSpec().ParseYAML(`
path: /testpost
tcp:
  reuse_port: true
`, nil)
	require.NoError(t, err)

	h1, err = gateway.InputFromParsed(pConf1, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, h1.Connect(tCtx))

	go func() {
		batch, aFn, err := h1.ReadBatch(tCtx)
		if err != nil {
			return
		}
		require.NoError(t, aFn(tCtx, nil))
		require.Len(t, batch, 1)
		close(received1)
	}()

	time.Sleep(100 * time.Millisecond)

	// Send request to first server
	res, err := http.Post(
		"http://127.0.0.1:19283/testpost",
		"application/octet-stream",
		bytes.NewBufferString("test message 1"),
	)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res.Body.Close()

	// Wait for message to be received
	select {
	case <-received1:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for first message")
	}

	// Close first server (releases port)
	closeCtx, closeDone := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeDone()
	require.NoError(t, h1.Close(closeCtx))

	// Small delay to ensure port is fully released
	time.Sleep(100 * time.Millisecond)

	// Create second server instance on the same address (simulating reload)
	pConf2, err := gateway.InputSpec().ParseYAML(`
path: /testpost
tcp:
  reuse_port: true
`, nil)
	require.NoError(t, err)

	h2, err := gateway.InputFromParsed(pConf2, service.MockResources())
	require.NoError(t, err)

	// This should succeed due to SO_REUSEADDR
	require.NoError(t, h2.Connect(tCtx), "Failed to bind to port after reload - this is the bug we're fixing")

	// Read handler goroutine for second server
	received2 := make(chan struct{})
	go func() {
		batch, aFn, err := h2.ReadBatch(tCtx)
		if err != nil {
			return
		}
		require.NoError(t, aFn(tCtx, nil))
		require.Len(t, batch, 1)
		close(received2)
	}()

	time.Sleep(100 * time.Millisecond)

	// Send request to second server - should work (not return 503)
	res, err = http.Post(
		"http://127.0.0.1:19283/testpost",
		"application/octet-stream",
		bytes.NewBufferString("test message 2"),
	)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode, "Server returned non-200 status after reload")
	res.Body.Close()

	// Wait for message to be received
	select {
	case <-received2:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for second message - server may not be accepting connections after reload")
	}

	// Cleanup
	require.NoError(t, h2.Close(closeCtx))
}
