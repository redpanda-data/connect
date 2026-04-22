// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// buildSalesforceGraphQLInput constructs a *salesforceGraphQLInput directly
// so tests can drive Connect/Read/Close without the AutoRetryNacks wrapper.
func buildSalesforceGraphQLInput(t *testing.T, mgr *service.Resources, yamlStr string) *salesforceGraphQLInput {
	t.Helper()

	pConf, err := salesforceGraphQLInputConfigSpec().ParseYAML(yamlStr, nil)
	require.NoError(t, err)
	conf, err := NewGraphQLInputConfigFromParsed(pConf)
	require.NoError(t, err)
	in := &salesforceGraphQLInput{
		conf:    conf,
		mgr:     mgr,
		logger:  mgr.Logger(),
		stopSig: shutdown.NewSignaller(),
	}
	in.stopSig.TriggerHasStopped()
	return in
}

// drainSalesforceGraphQLInput reads from the input until ErrEndOfInput, parses
// each message as JSON and returns the slice. Bounded to 60s.
func drainSalesforceGraphQLInput(t *testing.T, in *salesforceGraphQLInput) []map[string]any {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	var out []map[string]any
	for {
		msg, ack, err := in.Read(ctx)
		if errors.Is(err, service.ErrEndOfInput) {
			return out
		}
		require.NoError(t, err)
		b, err := msg.AsBytes()
		require.NoError(t, err)
		var m map[string]any
		require.NoError(t, json.Unmarshal(b, &m))
		out = append(out, m)
		require.NoError(t, ack(ctx, nil))
	}
}

func sfGraphQLBaseYAML(orgURL, clientID, clientSecret string) string {
	return fmt.Sprintf(`
org_url: %q
client_id: %q
client_secret: %q
`, orgURL, clientID, clientSecret)
}

// uiapiNodeID extracts the Id scalar from a UIAPI Account node. UIAPI returns
// Id as a bare ID! scalar (not a wrapped { value: ... } object like typed
// fields), so we read it directly. Returns "" on shape mismatch.
func uiapiNodeID(node map[string]any) string {
	id, _ := node["Id"].(string)
	return id
}

func TestIntegrationSalesforceGraphQLFilteredByID(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given a Salesforce org with a seeded Account")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	name := fmt.Sprintf("rpcn-it-gql-filter-%d", time.Now().UnixNano())
	id := createTestAccount(t, helper, name)

	t.Log("when the salesforce_graphql input runs a UIAPI query filtered by Id")
	mgr := newTestResources(t)
	yaml := sfGraphQLBaseYAML(orgURL, clientID, clientSecret) + fmt.Sprintf(`
query: |
  query AccountById($id: ID) {
    uiapi {
      query {
        Account(where: { Id: { eq: $id } }) {
          edges { node { Id Name { value } } }
          pageInfo { hasNextPage endCursor }
        }
      }
    }
  }
variables: |
  root = {"id": %q}
`, id)
	in := buildSalesforceGraphQLInput(t, mgr, yaml)
	require.NoError(t, in.Connect(t.Context()))
	defer in.Close(context.Background())
	rows := drainSalesforceGraphQLInput(t, in)

	t.Log("then exactly the seeded Account is returned and the input terminates")
	require.Len(t, rows, 1, "expected exactly the seeded Account")
	assert.Equal(t, id, uiapiNodeID(rows[0]), "row Id mismatch: %+v", rows[0])
}

func TestIntegrationSalesforceGraphQLPagination(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given five Accounts created with a unique name prefix")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	prefix := fmt.Sprintf("rpcn-it-gql-page-%d", time.Now().UnixNano())
	wantIDs := map[string]struct{}{}
	for i := range 5 {
		id := createTestAccount(t, helper, fmt.Sprintf("%s-%d", prefix, i))
		wantIDs[id] = struct{}{}
	}

	t.Log("when the salesforce_graphql input runs a paginated UIAPI query with first=2")
	// Page size 2 forces at least 3 pages over the 5 seeded rows. The query
	// filters by name LIKE so unrelated org records don't pollute the result.
	mgr := newTestResources(t)
	yaml := sfGraphQLBaseYAML(orgURL, clientID, clientSecret) + fmt.Sprintf(`
query: |
  query AccountsByPrefix($name: String, $first: Int) {
    uiapi {
      query {
        Account(where: { Name: { like: $name } }, first: $first, orderBy: { CreatedDate: { order: ASC } }) {
          edges { node { Id Name { value } } }
          pageInfo { hasNextPage endCursor }
        }
      }
    }
  }
variables: |
  root = {"name": %q, "first": 2}
`, prefix+"%")
	in := buildSalesforceGraphQLInput(t, mgr, yaml)
	require.NoError(t, in.Connect(t.Context()))
	defer in.Close(context.Background())
	rows := drainSalesforceGraphQLInput(t, in)

	t.Log("then every seeded Account appears in the output across multiple pages")
	gotIDs := map[string]struct{}{}
	for _, r := range rows {
		if id := uiapiNodeID(r); id != "" {
			gotIDs[id] = struct{}{}
		}
	}
	for id := range wantIDs {
		assert.Contains(t, gotIDs, id, "missing seeded Account %s — pagination did not reach it", id)
	}
}
