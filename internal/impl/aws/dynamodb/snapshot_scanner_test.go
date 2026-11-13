// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// scanStubTransport serves Scan: the first response per segment is a
// throttle error when throttleFirst is set, then one page of items. It also
// serves the checkpoint table: GetItem and Query return nothing (no snapshot
// progress) unless overridden, and PutItem and DeleteItem bodies are
// recorded, as are Query bodies. Deleting the snapshot#complete row clears
// the getItem override.
type scanStubTransport struct {
	mu            sync.Mutex
	throttleFirst bool
	calls         int
	consistent    []bool
	items         string // JSON array of items
	// onScan runs at the start of every Scan request, before it responds.
	onScan func()
	puts   []string
	// getItem overrides the GetItem response body when set.
	getItem string
	// query overrides the Query response body when set.
	query   string
	queries []string
	deletes []string
	// scanTables and scanStartKeys record each Scan's TableName and raw
	// ExclusiveStartKey JSON ("" when absent), in order.
	scanTables    []string
	scanStartKeys []string
	// scanItems, when set, picks a Scan's items from its raw
	// ExclusiveStartKey instead of items.
	scanItems func(startKey string) string
}

// queryBodies returns a copy of the recorded Query bodies, in order.
func (s *scanStubTransport) queryBodies() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.queries...)
}

// deleteBodies returns a copy of the recorded DeleteItem bodies, in order.
func (s *scanStubTransport) deleteBodies() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deletes...)
}

// putContains reports whether any recorded PutItem body contains substr.
func (s *scanStubTransport) putContains(substr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.puts {
		if strings.Contains(b, substr) {
			return true
		}
	}
	return false
}

func (s *scanStubTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	body, _ := io.ReadAll(req.Body)
	hdr := http.Header{}
	hdr.Set("Content-Type", "application/x-amz-json-1.0")
	ok := func(payload string) *http.Response {
		return &http.Response{
			StatusCode: 200, Header: hdr, Request: req,
			Body: io.NopCloser(strings.NewReader(payload)),
		}
	}
	switch {
	case strings.HasSuffix(target, ".GetItem"):
		s.mu.Lock()
		getItem := s.getItem
		s.mu.Unlock()
		if getItem != "" {
			return ok(getItem), nil
		}
		return ok(`{}`), nil
	case strings.HasSuffix(target, ".Query"):
		s.mu.Lock()
		s.queries = append(s.queries, string(body))
		s.mu.Unlock()
		if s.query != "" {
			return ok(s.query), nil
		}
		return ok(`{"Items":[]}`), nil
	case strings.HasSuffix(target, ".DeleteItem"):
		s.mu.Lock()
		s.deletes = append(s.deletes, string(body))
		if strings.Contains(string(body), "snapshot#complete") {
			s.getItem = ""
		}
		s.mu.Unlock()
		return ok(`{}`), nil
	case strings.HasSuffix(target, ".PutItem"):
		s.mu.Lock()
		s.puts = append(s.puts, string(body))
		s.mu.Unlock()
		return ok(`{}`), nil
	case !strings.HasSuffix(target, ".Scan"):
		return nil, fmt.Errorf("scanStubTransport: unexpected %q", target)
	}
	if s.onScan != nil {
		s.onScan()
	}
	var in struct {
		ConsistentRead    *bool
		TableName         string
		ExclusiveStartKey json.RawMessage
	}
	_ = json.Unmarshal(body, &in)

	s.mu.Lock()
	s.calls++
	call := s.calls
	s.consistent = append(s.consistent, in.ConsistentRead != nil && *in.ConsistentRead)
	s.scanTables = append(s.scanTables, in.TableName)
	s.scanStartKeys = append(s.scanStartKeys, string(in.ExclusiveStartKey))
	items := s.items
	if s.scanItems != nil {
		items = s.scanItems(string(in.ExclusiveStartKey))
	}
	s.mu.Unlock()

	if s.throttleFirst && call == 1 {
		hdr.Set("X-Amzn-ErrorType", "ProvisionedThroughputExceededException")
		return &http.Response{
			StatusCode: 400, Header: hdr, Request: req,
			Body: io.NopCloser(strings.NewReader(`{"__type":"ProvisionedThroughputExceededException","message":"stub"}`)),
		}, nil
	}
	return ok(fmt.Sprintf(`{"Items":%s}`, items)), nil
}

// scannedTables returns a copy of the recorded Scan table names.
func (s *scanStubTransport) scannedTables() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.scanTables...)
}

func newStubDynamoClient(t aws.HTTPClient) *dynamodb.Client {
	return dynamodb.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}}, func(o *dynamodb.Options) {
		o.HTTPClient = t
		o.Retryer = aws.NopRetryer{}
	})
}

func TestSnapshotScannerConsistentReadAndBeforeRequest(t *testing.T) {
	for _, consistent := range []bool{false, true} {
		t.Run(fmt.Sprintf("consistent=%v", consistent), func(t *testing.T) {
			tr := &scanStubTransport{throttleFirst: true, items: `[{"pk":{"S":"a"}}]`}
			s := NewSnapshotScanner(SnapshotScannerConfig{
				Client: newStubDynamoClient(tr), Table: "t", Segments: 1, BatchSize: 10,
				Throttle: time.Millisecond, ConsistentRead: consistent, Logger: service.MockResources().Logger(),
			})
			var before int
			s.SetBeforeRequestCallback(func(segment int) {
				assert.Equal(t, 0, segment)
				before++
			})
			s.SetBatchCallback(func(context.Context, DynamoItems, int, map[string]dynamodbtypes.AttributeValue) error { return nil })
			require.NoError(t, s.Scan(t.Context(), nil))
			assert.Equal(t, 2, before, "fires for the throttled request and the retry")
			assert.Equal(t, []bool{consistent, consistent}, tr.consistent)
		})
	}
}
