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

package turbopuffer

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tpuf "github.com/turbopuffer/turbopuffer-go/v2"
	"github.com/turbopuffer/turbopuffer-go/v2/option"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationTurbopufferOutput(t *testing.T) {
	integration.CheckSkip(t)

	apiKey := os.Getenv("TURBOPUFFER_API_KEY")
	if apiKey == "" {
		t.Skip("TURBOPUFFER_API_KEY is not set")
	}
	region := os.Getenv("TURBOPUFFER_REGION")
	if region == "" {
		region = "gcp-us-central1"
	}
	namespace := fmt.Sprintf("redpanda-connect-%d", time.Now().UnixNano())

	conf, err := outputSpec().ParseYAML(fmt.Sprintf(`
api_key: %q
region: %q
namespace: %q
operation: '${! meta("operation") }'
id: root = this.id
document_mapping: |
  root = this
  root.id = deleted()
max_retries: 2
`, apiKey, region, namespace), service.NewEnvironment())
	require.NoError(t, err)
	w, err := newOutputWriter(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, w.Connect(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, w.Close(context.Background()))
	})

	client := tpuf.NewClient(
		option.WithAPIKey(apiKey),
		option.WithRegion(region),
		option.WithMaxRetries(2),
	)
	ns := client.Namespace(namespace)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, deleteErr := ns.DeleteAll(cleanupCtx, tpuf.NamespaceDeleteAllParams{})
		if deleteErr != nil {
			t.Logf("failed to clean up turbopuffer namespace %q: %v", namespace, deleteErr)
		}
	})

	message := func(operation, title string) *service.Message {
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{
			"id":    "page-1",
			"title": title,
		})
		msg.MetaSetMut("operation", operation)
		return msg
	}

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{message(operationUpsert, "before")}))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{message(operationPatch, "after")}))

	query := func() *tpuf.NamespaceQueryResponse {
		res, queryErr := ns.Query(t.Context(), tpuf.NamespaceQueryParams{
			RankBy:            tpuf.NewRankByAttribute("id", "asc"),
			TopK:              tpuf.Int(10),
			IncludeAttributes: tpuf.IncludeAttributesParam{Bool: tpuf.Bool(true)},
			Consistency:       tpuf.NamespaceQueryParamsConsistency{Level: "strong"},
		})
		require.NoError(t, queryErr)
		return res
	}

	rows := query().Rows
	require.Len(t, rows, 1)
	assert.Equal(t, "page-1", rows[0]["id"])
	assert.Equal(t, "after", rows[0]["title"])

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{message(operationDelete, "")}))
	assert.Empty(t, query().Rows)
}
