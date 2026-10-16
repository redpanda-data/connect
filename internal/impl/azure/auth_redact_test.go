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

package azure

import (
	"context"
	"net"
	"net/url"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSASSig = "S3cretSignature"

// closedEndpoint returns the URL of a local port that nothing listens on.
func closedEndpoint(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return "http://" + addr + "/devstoreaccount1"
}

func noRetryContext(t *testing.T) context.Context {
	return policy.WithRetryOptions(t.Context(), policy.RetryOptions{MaxRetries: -1})
}

func assertRedactedTransportError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSASSig)
	var uErr *url.Error
	require.ErrorAs(t, err, &uErr)
	assert.NotContains(t, uErr.URL, testSASSig)
}

func TestStorageClientsRedactSASFromTransportErrors(t *testing.T) {
	endpoint := closedEndpoint(t)
	sas := "SharedAccessSignature=sv=2022-11-02&ss=bqt&srt=sco&sp=rl&sig=" + testSASSig

	t.Run("blob", func(t *testing.T) {
		client, _, err := getBlobStorageClient("BlobEndpoint="+endpoint+";"+sas, "devstoreaccount1", "", "", nil)
		require.NoError(t, err)
		_, err = client.ServiceClient().GetProperties(noRetryContext(t), nil)
		assertRedactedTransportError(t, err)
	})

	t.Run("data lake", func(t *testing.T) {
		client, _, err := getDLClient("BlobEndpoint="+endpoint+";"+sas, "devstoreaccount1", "", "", nil)
		require.NoError(t, err)
		_, err = client.GetProperties(noRetryContext(t), nil)
		assertRedactedTransportError(t, err)
	})

	t.Run("queue", func(t *testing.T) {
		client, err := getQueueServiceClient("devstoreaccount1", "", "QueueEndpoint="+endpoint+";"+sas, "")
		require.NoError(t, err)
		_, err = client.GetServiceProperties(noRetryContext(t), nil)
		assertRedactedTransportError(t, err)
	})

	t.Run("table", func(t *testing.T) {
		// The tables connection string parser ignores a custom endpoint for
		// SAS tokens, so the client is built as getTablesServiceClient does.
		client, err := aztables.NewServiceClientWithNoCredential(endpoint+"?sv=2022-11-02&sig="+testSASSig, tablesClientOptions())
		require.NoError(t, err)
		_, err = client.GetProperties(noRetryContext(t), nil)
		assertRedactedTransportError(t, err)
	})
}

func TestTablesClientBadAccessKeyError(t *testing.T) {
	_, err := getTablesServiceClient("account", "not base64!", "", "")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "%!v")
	assert.Contains(t, err.Error(), "invalid azure storage account credentials: ")
	assert.NotContains(t, err.Error(), "not base64!")
}
