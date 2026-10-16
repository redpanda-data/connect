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
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
