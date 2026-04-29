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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
)

const (
	testCDCCacheLabel    = "sf_cdc_cache"
	testCDCCheckpointKey = "salesforce_cdc"

	testPETopic = "/event/RPCN_Test__e"
)

// buildCDCInput constructs a *salesforceCDCInput directly (bypassing the
// registered constructor wrapping), giving tests access to WaitReady. The
// returned mgr can be reused across instances to share cache state.
func buildCDCInput(t *testing.T, mgr *service.Resources, yamlStr string) *salesforceCDCInput {
	t.Helper()

	pConf, err := salesforceCDCInputConfigSpec().ParseYAML(yamlStr, nil)
	require.NoError(t, err)

	conf, err := NewCDCInputConfigFromParsed(pConf)
	require.NoError(t, err)

	specs := make([]cdcTopicSpec, 0, len(conf.Topics))
	for _, raw := range conf.Topics {
		spec, err := parseCDCTopic(raw)
		require.NoError(t, err)
		specs = append(specs, spec)
	}

	batching, err := pConf.FieldBatchPolicy(sfFieldBatching)
	require.NoError(t, err)
	if batching.IsNoop() {
		batching.Count = 1
	}

	return &salesforceCDCInput{
		conf:       conf,
		topicSpecs: specs,
		batching:   batching,
		mgr:        mgr,
		logger:     mgr.Logger(),
	}
}

// readCDCPersistedState fetches the CDC input's checkpoint state from the cache
// and unmarshals it. Returns (state, false) when the key is missing.
func readCDCPersistedState(t *testing.T, mgr *service.Resources, cacheLabel, key string) (executorState, bool) {
	t.Helper()
	raw, ok := readCacheBytes(t, mgr, cacheLabel, key)
	if !ok {
		return executorState{}, false
	}
	var st executorState
	require.NoError(t, json.Unmarshal(raw, &st))
	return st, true
}

func cdcBaseYAML(orgURL, clientID, clientSecret string) string {
	return fmt.Sprintf(`
org_url: %q
client_id: %q
client_secret: %q
checkpoint_cache: %s
checkpoint_cache_key: %s
`, orgURL, clientID, clientSecret, testCDCCacheLabel, testCDCCheckpointKey)
}

// publishTestEvent publishes a RPCN_Test__e Platform Event with the given
// message body via the REST API. Returns the event's "id" field.
func publishTestEvent(t *testing.T, client *salesforcehttp.Client, message string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"Message__c": message})
	require.NoError(t, err)
	raw, err := client.PostJSON(t.Context(), client.SObjectPath("RPCN_Test__e"), body)
	require.NoError(t, err, "publish RPCN_Test__e")
	var resp struct {
		ID      string `json:"id"`
		Success bool   `json:"success"`
	}
	require.NoError(t, json.Unmarshal(raw, &resp))
	require.True(t, resp.Success, "publish response not success: %s", string(raw))
	t.Logf("published RPCN_Test__e id=%s message=%q", resp.ID, message)
	return resp.ID
}

// hasTopicMessage returns true when any captured event on `topic` carries the
// expected Message__c value.
func hasTopicMessage(msgs []capturedMessage, topic, wantMessage string) bool {
	for _, m := range msgs {
		if m.topic != topic {
			continue
		}
		if peMessageEquals(m.bodyJSON, wantMessage) {
			return true
		}
	}
	return false
}

// peMessageEquals decodes a Platform Event payload and reports whether its
// Message__c equals want. Salesforce Avro decoding may wrap the field in a
// union ({"string": s}) so both shapes are accepted.
func peMessageEquals(body []byte, want string) bool {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return false
	}
	v, ok := m["Message__c"]
	if !ok {
		return false
	}
	switch x := v.(type) {
	case string:
		return x == want
	case map[string]any:
		if s, ok := x["string"].(string); ok {
			return s == want
		}
	}
	return false
}

func TestIntegrationSalesforceCDCSnapshotCompletes(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given a Salesforce org with the Account sObject CDC-enabled")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	mgr := newTestResources(t, testCDCCacheLabel)

	t.Log("when salesforce_cdc runs with topics=[Account] and stream_snapshot=true")
	yaml := cdcBaseYAML(orgURL, clientID, clientSecret) + `
topics: [Account]
stream_snapshot: true
replay_preset: latest
snapshot_max_batch_size: 200
`
	in := buildCDCInput(t, mgr, yaml)
	stop, get := startAndCollectBatch(t, in)
	defer stop()

	t.Log("then the snapshot phase emits Account rows tagged operation=read")
	require.Eventually(t, func() bool {
		for _, m := range get() {
			if m.operation == "read" && m.sobject == "Account" {
				return true
			}
		}
		return false
	}, 60*time.Second, 250*time.Millisecond, "expected at least one snapshot Account row")

	for _, m := range get() {
		if m.operation != "read" || m.sobject != "Account" {
			continue
		}
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(m.bodyJSON, &parsed), "row must be JSON: %s", m.bodyJSON)
		assert.Contains(t, parsed, "Id", "row missing Id: %v", parsed)
		break
	}
}

func TestIntegrationSalesforceCDCSnapshotThenCDCCreate(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given salesforce_cdc with topics=[Account] subscribed and ready")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	mgr := newTestResources(t, testCDCCacheLabel)

	yaml := cdcBaseYAML(orgURL, clientID, clientSecret) + `
topics: [Account]
stream_snapshot: true
replay_preset: latest
snapshot_max_batch_size: 200
`
	in := buildCDCInput(t, mgr, yaml)
	stop, get := startAndCollectBatch(t, in)
	defer stop()

	readyCtx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	require.NoError(t, in.WaitReady(readyCtx), "input did not become ready in time")

	t.Log("when an Account is created via REST after subscription is live")
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	name := fmt.Sprintf("rpcn-it-cdc-%d", time.Now().UnixNano())
	id := createTestAccount(t, helper, name)

	t.Log("then a CDC create event for that Account arrives on the Pub/Sub stream")
	require.Eventually(t, func() bool {
		return hasOp(get(), "create", "Account", id)
	}, 90*time.Second, 500*time.Millisecond, "no create event for Account id=%s", id)
}

func TestIntegrationSalesforceCDCCDCOnlySkipSnapshot(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given salesforce_cdc with topics=[Account] and stream_snapshot=false")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	mgr := newTestResources(t, testCDCCacheLabel)

	yaml := cdcBaseYAML(orgURL, clientID, clientSecret) + `
topics: [Account]
stream_snapshot: false
replay_preset: latest
`
	in := buildCDCInput(t, mgr, yaml)
	stop, get := startAndCollectBatch(t, in)
	defer stop()

	readyCtx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	require.NoError(t, in.WaitReady(readyCtx))

	t.Log("when an Account is created after the input is ready")
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	name := fmt.Sprintf("rpcn-it-cdc-skip-%d", time.Now().UnixNano())
	id := createTestAccount(t, helper, name)

	t.Log("then the CDC create event arrives and no snapshot rows are emitted")
	require.Eventually(t, func() bool {
		return hasOp(get(), "create", "Account", id)
	}, 90*time.Second, 500*time.Millisecond, "no create event for Account id=%s", id)
	for _, m := range get() {
		assert.NotEqualf(t, "read", m.operation, "snapshot was disabled but got read op: sobject=%s", m.sobject)
	}
}

func TestIntegrationSalesforceCDCCheckpointResume(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given salesforce_cdc with topics=[Account] running in a first phase")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	mgr := newTestResources(t, testCDCCacheLabel)
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)

	yaml := cdcBaseYAML(orgURL, clientID, clientSecret) + `
topics: [Account]
stream_snapshot: false
replay_preset: latest
`
	inA := buildCDCInput(t, mgr, yaml)
	stopA, getA := startAndCollectBatch(t, inA)

	readyCtx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	require.NoError(t, inA.WaitReady(readyCtx))
	cancel()

	t.Log("when an Account is created and acked, then the input is closed")
	name1 := fmt.Sprintf("rpcn-it-cdc-resume1-%d", time.Now().UnixNano())
	id1 := createTestAccount(t, helper, name1)
	require.Eventually(t, func() bool {
		return hasOp(getA(), "create", "Account", id1)
	}, 90*time.Second, 500*time.Millisecond, "phase1: no create event for %s", id1)
	stopA()

	t.Log("then the cache holds snapshot_complete and a per-topic replay ID")
	st, ok := readCDCPersistedState(t, mgr, testCDCCacheLabel, testCDCCheckpointKey)
	require.True(t, ok, "checkpoint cache should be populated after phase 1")
	assert.True(t, st.SnapshotComplete, "snapshot_complete should be true")
	require.NotEmpty(t, st.Topics["/data/AccountChangeEvent"], "replay_id for /data/AccountChangeEvent should be persisted after phase 1")

	t.Log("when a fresh input starts and a new Account is created")
	inB := buildCDCInput(t, mgr, yaml)
	stopB, getB := startAndCollectBatch(t, inB)
	defer stopB()

	readyCtx2, cancel2 := context.WithTimeout(t.Context(), 60*time.Second)
	require.NoError(t, inB.WaitReady(readyCtx2))
	cancel2()

	name2 := fmt.Sprintf("rpcn-it-cdc-resume2-%d", time.Now().UnixNano())
	id2 := createTestAccount(t, helper, name2)

	t.Log("then phase 2 receives the new event after resuming from the persisted replay")
	require.Eventually(t, func() bool {
		return hasOp(getB(), "create", "Account", id2)
	}, 90*time.Second, 500*time.Millisecond, "phase2: no create event for %s", id2)
}

func TestIntegrationSalesforceCDCMultiObjectFirehose(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given salesforce_cdc with topics=[/data/ChangeEvents] subscribing to the firehose")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	mgr := newTestResources(t, testCDCCacheLabel)

	yaml := cdcBaseYAML(orgURL, clientID, clientSecret) + `
topics: [/data/ChangeEvents]
stream_snapshot: false
replay_preset: latest
`
	in := buildCDCInput(t, mgr, yaml)
	stop, get := startAndCollectBatch(t, in)
	defer stop()

	readyCtx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	require.NoError(t, in.WaitReady(readyCtx))

	t.Log("when an Account and a Contact are created concurrently")
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	accountID := createTestAccount(t, helper, fmt.Sprintf("rpcn-it-firehose-acc-%d", time.Now().UnixNano()))
	contactID := createTestContact(t, helper, fmt.Sprintf("rpcn-it-firehose-con-%d", time.Now().UnixNano()))

	t.Log("then both create events arrive through the firehose")
	require.Eventually(t, func() bool {
		msgs := get()
		return hasOp(msgs, "create", "Account", accountID) && hasOp(msgs, "create", "Contact", contactID)
	}, 120*time.Second, 500*time.Millisecond, "firehose: missing Account %s or Contact %s create", accountID, contactID)
}

func TestIntegrationSalesforceCDCLifecycle(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given salesforce_cdc with topics=[Account] subscribed and ready")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	mgr := newTestResources(t, testCDCCacheLabel)

	yaml := cdcBaseYAML(orgURL, clientID, clientSecret) + `
topics: [Account]
stream_snapshot: false
replay_preset: latest
`
	in := buildCDCInput(t, mgr, yaml)
	stop, get := startAndCollectBatch(t, in)
	defer stop()

	readyCtx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	require.NoError(t, in.WaitReady(readyCtx))

	t.Log("when an Account is created, updated, then deleted via REST")
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	name := fmt.Sprintf("rpcn-it-lifecycle-%d", time.Now().UnixNano())
	id := createTestAccount(t, helper, name)

	require.Eventually(t, func() bool {
		return hasOp(get(), "create", "Account", id)
	}, 90*time.Second, 500*time.Millisecond, "no create event for Account id=%s", id)

	updateTestAccountName(t, helper, id, name+"-updated")
	require.Eventually(t, func() bool {
		return hasOp(get(), "update", "Account", id)
	}, 90*time.Second, 500*time.Millisecond, "no update event for Account id=%s", id)

	deleteTestAccount(t, helper, id)

	t.Log("then create, update, and delete CDC events all arrive")
	require.Eventually(t, func() bool {
		return hasOp(get(), "delete", "Account", id)
	}, 90*time.Second, 500*time.Millisecond, "no delete event for Account id=%s", id)
}

func TestIntegrationSalesforceCDCMultiTopicParallel(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given salesforce_cdc with topics=[Account, Contact] running in parallel")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	mgr := newTestResources(t, testCDCCacheLabel)

	yaml := cdcBaseYAML(orgURL, clientID, clientSecret) + `
topics: [Account, Contact]
stream_snapshot: false
replay_preset: latest
`
	in := buildCDCInput(t, mgr, yaml)
	stop, get := startAndCollectBatch(t, in)
	defer stop()

	readyCtx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	require.NoError(t, in.WaitReady(readyCtx))

	t.Log("when an Account and a Contact are created")
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	accID := createTestAccount(t, helper, fmt.Sprintf("rpcn-it-multi-acc-%d", time.Now().UnixNano()))
	conID := createTestContact(t, helper, fmt.Sprintf("rpcn-it-multi-con-%d", time.Now().UnixNano()))

	t.Log("then events flow on both per-sObject subscriptions on a shared gRPC client")
	require.Eventually(t, func() bool {
		msgs := get()
		return hasOp(msgs, "create", "Account", accID) && hasOp(msgs, "create", "Contact", conID)
	}, 120*time.Second, 500*time.Millisecond, "multi-topic: missing Account %s or Contact %s create", accID, conID)

	// Per-topic checkpoints recorded under their full topic paths.
	st, ok := readCDCPersistedState(t, mgr, testCDCCacheLabel, testCDCCheckpointKey)
	require.True(t, ok, "checkpoint cache should be populated")
	assert.NotEmpty(t, st.Topics["/data/AccountChangeEvent"], "Account topic replay missing")
	assert.NotEmpty(t, st.Topics["/data/ContactChangeEvent"], "Contact topic replay missing")
}

func TestIntegrationSalesforceCDCPlatformEvent(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given salesforce_cdc with topics=[/event/RPCN_Test__e] (Platform Event only)")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	mgr := newTestResources(t, testCDCCacheLabel)

	yaml := cdcBaseYAML(orgURL, clientID, clientSecret) + fmt.Sprintf(`
topics: [%s]
stream_snapshot: false
replay_preset: latest
`, testPETopic)

	in := buildCDCInput(t, mgr, yaml)
	stop, get := startAndCollectBatch(t, in)
	defer stop()

	readyCtx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	require.NoError(t, in.WaitReady(readyCtx))

	t.Log("when a tagged RPCN_Test__e event is published via REST")
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	want := fmt.Sprintf("rpcn-it-cdc-pe-%d", time.Now().UnixNano())
	publishTestEvent(t, helper, want)

	t.Log("then the event arrives with topic / replay_id metadata")
	require.Eventually(t, func() bool {
		return hasTopicMessage(get(), testPETopic, want)
	}, 60*time.Second, 500*time.Millisecond, "no event with Message__c=%q on %s", want, testPETopic)

	// event_uuid is only populated for standard Platform Events (e.g.
	// LoginEventStream); Salesforce does not include EventUuid in the Avro
	// payload of custom (__e) Platform Events, so we do not assert it here.
	for _, m := range get() {
		if m.topic != testPETopic || !peMessageEquals(m.bodyJSON, want) {
			continue
		}
		assert.NotEmpty(t, m.replayID, "replay_id meta missing")
		return
	}
	t.Fatal("matching event was reported by hasTopicMessage but not found on second pass")
}

func TestIntegrationSalesforceCDCMixedCDCAndPlatformEvent(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("given salesforce_cdc with topics=[Account, /event/RPCN_Test__e]")
	orgURL, clientID, clientSecret := requireSalesforceEnv(t)
	mgr := newTestResources(t, testCDCCacheLabel)

	yaml := cdcBaseYAML(orgURL, clientID, clientSecret) + fmt.Sprintf(`
topics: [Account, %s]
stream_snapshot: false
replay_preset: latest
`, testPETopic)

	in := buildCDCInput(t, mgr, yaml)
	stop, get := startAndCollectBatch(t, in)
	defer stop()

	readyCtx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	require.NoError(t, in.WaitReady(readyCtx))

	t.Log("when an Account is created and a Platform Event is published")
	helper := newTestSalesforceClient(t, orgURL, clientID, clientSecret)
	accID := createTestAccount(t, helper, fmt.Sprintf("rpcn-it-mixed-acc-%d", time.Now().UnixNano()))
	wantMsg := fmt.Sprintf("rpcn-it-mixed-pe-%d", time.Now().UnixNano())
	publishTestEvent(t, helper, wantMsg)

	t.Log("then both the CDC event and the Platform Event arrive on the same input")
	require.Eventually(t, func() bool {
		msgs := get()
		return hasOp(msgs, "create", "Account", accID) && hasTopicMessage(msgs, testPETopic, wantMsg)
	}, 120*time.Second, 500*time.Millisecond, "mixed: missing Account %s create or PE Message__c=%q", accID, wantMsg)
}
