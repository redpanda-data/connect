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
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// testAPIVersion is the Salesforce REST API version used by every integration
// test in this package.
const testAPIVersion = "v65.0"

// requireSalesforceEnv loads the three SALESFORCE_* env vars or skips the
// test when any are missing. Returned values are non-empty.
func requireSalesforceEnv(t *testing.T) (orgURL, clientID, clientSecret string) {
	t.Helper()
	orgURL = os.Getenv("SALESFORCE_ORG_URL")
	clientID = os.Getenv("SALESFORCE_CLIENT_ID")
	clientSecret = os.Getenv("SALESFORCE_CLIENT_SECRET")
	if orgURL == "" || clientID == "" || clientSecret == "" {
		t.Skip("SALESFORCE_ORG_URL, SALESFORCE_CLIENT_ID and SALESFORCE_CLIENT_SECRET must all be set")
	}
	return
}

// newTestSalesforceClient builds an authenticated Salesforce HTTP client for
// integration-test fixtures (creating/updating/deleting records).
func newTestSalesforceClient(t *testing.T, orgURL, clientID, clientSecret string) *salesforcehttp.Client {
	t.Helper()
	c, err := salesforcehttp.NewClient(salesforcehttp.ClientConfig{
		OrgURL:         orgURL,
		ClientID:       clientID,
		ClientSecret:   clientSecret,
		APIVersion:     testAPIVersion,
		QueryBatchSize: 2000,
		HTTPClient:     &http.Client{Timeout: 30 * time.Second},
		Logger:         service.MockResources().Logger(),
	})
	require.NoError(t, err)
	require.NoError(t, c.RefreshToken(t.Context()))
	return c
}

// newTestResources returns a MockResources with the named cache labels
// registered and an injected enterprise license for tests that exercise
// license-gated inputs. Inputs that don't need a cache may pass no labels.
func newTestResources(t *testing.T, cacheLabels ...string) *service.Resources {
	t.Helper()
	opts := make([]service.MockResourcesOptFn, 0, len(cacheLabels))
	for _, label := range cacheLabels {
		opts = append(opts, service.MockResourcesOptAddCache(label))
	}
	mgr := service.MockResources(opts...)
	license.InjectTestService(mgr)
	return mgr
}

// createTestAccount posts an Account and registers a delete cleanup. The
// cleanup tolerates 404s (which happen when a test deletes the record itself
// as part of lifecycle coverage).
func createTestAccount(t *testing.T, client *salesforcehttp.Client, name string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"Name": name})
	require.NoError(t, err)

	path := client.SObjectPath("Account")
	raw, err := client.PostJSON(t.Context(), path, body)
	require.NoError(t, err, "create Account")

	var resp struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(raw, &resp))
	require.NotEmpty(t, resp.ID, "create-Account response missing id: %s", string(raw))
	t.Logf("created Account name=%q id=%s", name, resp.ID)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := client.DeleteJSON(ctx, client.OrgURL()+path+"/"+resp.ID); err != nil {
			t.Logf("delete Account id=%s during cleanup: %v", resp.ID, err)
		}
	})
	return resp.ID
}

// updateTestAccountName patches an Account's Name field.
func updateTestAccountName(t *testing.T, client *salesforcehttp.Client, id, newName string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"Name": newName})
	require.NoError(t, err)
	_, err = client.PatchJSON(t.Context(), client.SObjectPath("Account")+"/"+id, body)
	require.NoError(t, err, "update Account %s", id)
}

// deleteTestAccount removes an Account by ID.
func deleteTestAccount(t *testing.T, client *salesforcehttp.Client, id string) {
	t.Helper()
	path := client.SObjectPath("Account")
	_, err := client.DeleteJSON(t.Context(), client.OrgURL()+path+"/"+id)
	require.NoError(t, err, "delete Account %s", id)
}

// createTestContact posts a Contact and registers a delete cleanup.
func createTestContact(t *testing.T, client *salesforcehttp.Client, lastName string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"LastName": lastName})
	require.NoError(t, err)

	path := client.SObjectPath("Contact")
	raw, err := client.PostJSON(t.Context(), path, body)
	require.NoError(t, err, "create Contact")

	var resp struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(raw, &resp))
	require.NotEmpty(t, resp.ID, "create-Contact response missing id: %s", string(raw))
	t.Logf("created Contact lastName=%q id=%s", lastName, resp.ID)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := client.DeleteJSON(ctx, client.OrgURL()+path+"/"+resp.ID); err != nil {
			t.Logf("delete Contact id=%s during cleanup: %v", resp.ID, err)
		}
	})
	return resp.ID
}

// capturedMessage mirrors the subset of message surface that integration tests
// commonly assert on. Fields not relevant to a particular test are simply
// left empty.
type capturedMessage struct {
	operation string
	sobject   string
	topic     string
	replayID  string
	recordIDs string
	eventUUID string
	bodyJSON  []byte
}

// startAndCollectBatch invokes Connect on a batch input and spawns a consumer
// goroutine that drains its batches, capturing each message's common
// metadata. Returns a stop function that closes the input and a mutex-guarded
// accessor for the collected messages.
func startAndCollectBatch(t *testing.T, in service.BatchInput) (stop func(), get func() []capturedMessage) {
	t.Helper()

	require.NoError(t, in.Connect(t.Context()))

	var mu sync.Mutex
	var msgs []capturedMessage

	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			batch, ack, err := in.ReadBatch(consumeCtx)
			if err != nil {
				return
			}
			mu.Lock()
			for _, msg := range batch {
				var c capturedMessage
				c.operation, _ = msg.MetaGet("operation")
				c.sobject, _ = msg.MetaGet("sobject")
				c.topic, _ = msg.MetaGet("topic")
				c.replayID, _ = msg.MetaGet("replay_id")
				c.recordIDs, _ = msg.MetaGet("record_ids")
				c.eventUUID, _ = msg.MetaGet("event_uuid")
				if b, err := msg.AsBytes(); err == nil {
					c.bodyJSON = append([]byte(nil), b...)
				}
				msgs = append(msgs, c)
			}
			mu.Unlock()
			_ = ack(consumeCtx, nil)
		}
	}()

	stop = func() {
		consumeCancel()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = in.Close(ctx)
		<-done
	}
	get = func() []capturedMessage {
		mu.Lock()
		defer mu.Unlock()
		out := make([]capturedMessage, len(msgs))
		copy(out, msgs)
		return out
	}
	return stop, get
}

// hasOp scans collected CDC messages for an op/sobject pair, optionally
// requiring a record ID to appear in the comma-separated `record_ids` meta.
func hasOp(msgs []capturedMessage, op, sobject, recordID string) bool {
	for _, m := range msgs {
		if m.operation != op || m.sobject != sobject {
			continue
		}
		if recordID == "" || strings.Contains(m.recordIDs, recordID) {
			return true
		}
	}
	return false
}

// readCacheBytes fetches a key from a named cache. It returns (raw, true) on
// hit, ([], false) on ErrKeyNotFound, and fails the test on any other error.
func readCacheBytes(t *testing.T, mgr *service.Resources, cacheLabel, key string) ([]byte, bool) {
	t.Helper()
	var raw []byte
	var getErr error
	require.NoError(t, mgr.AccessCache(t.Context(), cacheLabel, func(cache service.Cache) {
		raw, getErr = cache.Get(t.Context(), key)
	}))
	if errors.Is(getErr, service.ErrKeyNotFound) {
		return nil, false
	}
	require.NoError(t, getErr)
	if len(raw) == 0 {
		return nil, false
	}
	return raw, true
}
