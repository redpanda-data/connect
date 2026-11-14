// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streaming

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/typed"
)

const testSecret = "S3cretToken"

func TestRedactedBody(t *testing.T) {
	body := `{"prefix":"abc","status_code":0,"client_sequencer":9007199254740993,"stage_location":{"locationType":"S3","location":"bucket/path","creds":{"AWS_KEY_ID":"id","AWS_SECRET_KEY":"` + testSecret + `"},"presignedUrl":"https://x?sig=` + testSecret + `"},"channels":[{"iceberg_location":{"creds":{"AZURE_SAS_TOKEN":"` + testSecret + `"}}}]}`

	got := redactedBody(body).String()
	assert.NotContains(t, got, testSecret)
	assert.Contains(t, got, `"location":"bucket/path"`)
	assert.Contains(t, got, `"client_sequencer":9007199254740993`)
	assert.Contains(t, got, `"creds":"xxxxx"`)

	assert.Equal(t, "<invalid JSON omitted>", redactedBody("creds: "+testSecret).String())
}

func TestRedactedBodyEncryptionKey(t *testing.T) {
	body := `{"channel":"c","encryption_key":"` + testSecret + `","encryption_key_id":42}`

	got := redactedBody(body).String()
	assert.NotContains(t, got, testSecret)
	assert.Contains(t, got, `"encryption_key":"xxxxx"`)
	assert.Contains(t, got, `"encryption_key_id":42`)
}

func TestDoPostErrorsRedactBody(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		kept   string
	}{
		{name: "non 200", status: http.StatusInternalServerError, body: "creds: " + testSecret, kept: "status code (500)"},
		{name: "invalid response", status: http.StatusOK, body: `{"prefix":42,"stage_location":{"creds":{"AWS_SECRET_KEY":"` + testSecret + `"}}}`, kept: `"prefix":42`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			c := &SnowflakeRestClient{client: srv.Client(), cachedJWT: typed.NewAtomicValue("")}
			var resp struct {
				Prefix string `json:"prefix"`
			}
			err := c.doPost(t.Context(), srv.URL, struct{}{}, &resp)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), testSecret)
			assert.Contains(t, err.Error(), tc.kept)
		})
	}
}

func TestAzureUploaderRedactsSASToken(t *testing.T) {
	_, err := newUploader(fileLocationInfo{
		LocationType:   "AZURE",
		Location:       "container/path",
		StorageAccount: "bad host",
		EndPoint:       "blob.core.windows.net",
		Creds:          map[string]string{"AZURE_SAS_TOKEN": "?sv=2022-11-02&sig=" + testSecret},
	})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSecret)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(l.Addr().String())
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// The URL is built as https://<account>.<endpoint>/<token>.
	i := strings.LastIndexByte(host, '.')
	up, err := newUploader(fileLocationInfo{
		LocationType:   "AZURE",
		Location:       "container/path",
		StorageAccount: host[:i],
		EndPoint:       host[i+1:] + ":" + port,
		Creds:          map[string]string{"AZURE_SAS_TOKEN": "?sv=2022-11-02&sig=" + testSecret},
	})
	require.NoError(t, err)

	ctx := policy.WithRetryOptions(t.Context(), policy.RetryOptions{MaxRetries: -1})
	err = up.upload(ctx, "file", []byte("data"), nil, nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSecret)
}
