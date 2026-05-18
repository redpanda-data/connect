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

// buildSalesforceInput parses YAML and constructs a *salesforceInput directly
// so tests can call Connect/Read/Close without the AutoRetryNacks wrapper.
func buildSalesforceInput(t *testing.T, mgr *service.Resources, yamlStr string) *salesforceInput {
	t.Helper()

	pConf, err := salesforceInputConfigSpec().ParseYAML(yamlStr, nil)
	require.NoError(t, err)
	conf, err := NewInputConfigFromParsed(pConf)
	require.NoError(t, err)
	in := &salesforceInput{
		conf:    conf,
		mgr:     mgr,
		logger:  mgr.Logger(),
		stopSig: shutdown.NewSignaller(),
	}
	in.stopSig.TriggerHasStopped()
	return in
}

// drainSalesforceInput reads from the input until ErrEndOfInput, parses each
// message as JSON and returns the slice. It fails the test on any other error
// or when reading takes longer than 60s.
func drainSalesforceInput(t *testing.T, in *salesforceInput) []map[string]any {
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

func sfInputBaseYAML(orgURL, clientID, clientSecret string) string {
	return fmt.Sprintf(`
org_url: %q
client_id: %q
client_secret: %q
`, orgURL, clientID, clientSecret)
}

func TestIntegrationSalesforceFilteredByID(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given a Salesforce org with a seeded Account")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	name := fmt.Sprintf("rpcn-it-sf-filter-%d", time.Now().UnixNano())
	id := createTestAccount(t, helper, name)

	t.Log("when the salesforce input runs SELECT ... WHERE Id = ? bound to that ID")
	mgr := newTestResources(t)
	yaml := sfInputBaseYAML(orgURL, clientID, clientSecret) + fmt.Sprintf(`
object: Account
columns: [Id, Name]
where: Id = ?
args_mapping: |
  root = [%q]
`, id)
	in := buildSalesforceInput(t, mgr, yaml)
	require.NoError(t, in.Connect(t.Context()))
	defer in.Close(context.Background())
	rows := drainSalesforceInput(t, in)

	t.Log("then exactly the seeded Account is returned and the input terminates")
	require.Len(t, rows, 1, "expected exactly the seeded Account")
	assert.Equal(t, id, rows[0]["Id"])
	assert.Equal(t, name, rows[0]["Name"])
}

func TestIntegrationSalesforceLikePrefix(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given three Accounts created with a unique name prefix")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	prefix := fmt.Sprintf("rpcn-it-sf-like-%d", time.Now().UnixNano())
	wantIDs := map[string]struct{}{}
	for i := range 3 {
		id := createTestAccount(t, helper, fmt.Sprintf("%s-%d", prefix, i))
		wantIDs[id] = struct{}{}
	}

	t.Log("when the salesforce input runs SELECT ... WHERE Name LIKE 'prefix%'")
	mgr := newTestResources(t)
	yaml := sfInputBaseYAML(orgURL, clientID, clientSecret) + fmt.Sprintf(`
object: Account
columns: [Id, Name]
where: Name LIKE ?
args_mapping: |
  root = [%q]
suffix: ORDER BY CreatedDate ASC
`, prefix+"%")
	in := buildSalesforceInput(t, mgr, yaml)
	require.NoError(t, in.Connect(t.Context()))
	defer in.Close(context.Background())
	rows := drainSalesforceInput(t, in)

	t.Log("then every seeded Account appears in the output and the input terminates")
	gotIDs := map[string]struct{}{}
	for _, r := range rows {
		if id, ok := r["Id"].(string); ok {
			gotIDs[id] = struct{}{}
		}
	}
	for id := range wantIDs {
		assert.Contains(t, gotIDs, id, "missing seeded Account %s", id)
	}
}

func TestIntegrationSalesforceNoFilter(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given an Account seeded into a Salesforce org")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	name := fmt.Sprintf("rpcn-it-sf-nofilter-%d", time.Now().UnixNano())
	id := createTestAccount(t, helper, name)

	t.Log("when the salesforce input runs an unfiltered SELECT bounded by LIMIT 50")
	mgr := newTestResources(t)
	yaml := sfInputBaseYAML(orgURL, clientID, clientSecret) + `
object: Account
columns: [Id, Name]
suffix: ORDER BY CreatedDate DESC LIMIT 50
`
	in := buildSalesforceInput(t, mgr, yaml)
	require.NoError(t, in.Connect(t.Context()))
	defer in.Close(context.Background())
	rows := drainSalesforceInput(t, in)

	t.Log("then the seeded Account is among the returned rows and the input terminates")
	require.NotEmpty(t, rows)
	found := false
	for _, r := range rows {
		if r["Id"] == id {
			found = true
			assert.Equal(t, name, r["Name"])
			break
		}
	}
	assert.True(t, found, "seeded Account %s not in first 50 rows", id)
}
