// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package convertserver

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertEndpointHappy(t *testing.T) {
	srv := httptest.NewServer(newMux())
	defer srv.Close()

	body := `{"name":"s3","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"b","topics":"orders"}}`
	resp, err := http.Post(srv.URL+"/convert", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		YAML     string `json:"yaml"`
		Warnings []struct {
			Field   string `json:"field"`
			Message string `json:"message"`
		} `json:"warnings"`
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out.Error)
	assert.Contains(t, out.YAML, "aws_s3:")
	assert.Contains(t, out.YAML, "bucket: b")
}

func TestConvertEndpointError(t *testing.T) {
	srv := httptest.NewServer(newMux())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/convert", "application/json", strings.NewReader(`{not json`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		YAML  string `json:"yaml"`
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.NotEmpty(t, out.Error)
	assert.Empty(t, out.YAML)
}

func TestConvertEndpointWrongMethod(t *testing.T) {
	srv := httptest.NewServer(newMux())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/convert")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestIndexServesPage(t *testing.T) {
	srv := httptest.NewServer(newMux())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	page := string(b)
	assert.Contains(t, page, `id="input"`)
	assert.Contains(t, page, `id="output"`)
}

func TestCommandBuilds(t *testing.T) {
	cmd := Command()
	require.NotNil(t, cmd)
	assert.Equal(t, "server", cmd.Name)
}
