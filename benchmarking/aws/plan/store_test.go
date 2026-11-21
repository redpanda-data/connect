// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// fakeS3 is an in-memory S3API with S3's conditional-write semantics.
type fakeS3 struct {
	mu    sync.Mutex
	objs  map[string][]byte
	etags map[string]string
	ver   int
	calls int
	// afterGet, when set, runs after each GetObject of the given key (outside
	// the lock), so a test can interleave a competing writer.
	afterGet map[string]func()
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objs: map[string][]byte{}, etags: map[string]string{}}
}

type apiErr struct{ code string }

func (e apiErr) Error() string                 { return e.code }
func (e apiErr) ErrorCode() string             { return e.code }
func (e apiErr) ErrorMessage() string          { return e.code }
func (e apiErr) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	key := aws.ToString(in.Key)
	cur, exists := f.etags[key]
	if aws.ToString(in.IfNoneMatch) == "*" && exists {
		return nil, apiErr{"PreconditionFailed"}
	}
	if m := aws.ToString(in.IfMatch); m != "" && (!exists || m != cur) {
		return nil, apiErr{"PreconditionFailed"}
	}
	f.ver++
	f.objs[key] = b
	f.etags[key] = fmt.Sprintf("\"v%d\"", f.ver)
	return &s3.PutObjectOutput{ETag: aws.String(f.etags[key])}, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	f.calls++
	key := aws.ToString(in.Key)
	b, ok := f.objs[key]
	etag := f.etags[key]
	hook := f.afterGet[key]
	f.mu.Unlock()
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	if hook != nil {
		defer hook()
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b)), ETag: aws.String(etag)}, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	key := aws.ToString(in.Key)
	if m := aws.ToString(in.IfMatch); m != "" && m != f.etags[key] {
		return nil, apiErr{"PreconditionFailed"}
	}
	delete(f.objs, key)
	delete(f.etags, key)
	return &s3.DeleteObjectOutput{}, nil
}

func testPlan(id string) Plan {
	return Plan{ID: id, CreatedAt: time.Unix(1700000000, 0).UTC(), Runnable: true, Input: "http_server", Output: "drop", Processors: []string{}, Caveats: []string{}}
}

// stores returns every Store implementation, so each behaviour is checked
// against the S3 store (on the fake) and the directory store alike.
func stores(t *testing.T) map[string]Store {
	return map[string]Store{
		"s3":  NewS3Store(newFakeS3(), "bucket"),
		"dir": NewDirStore(t.TempDir()),
	}
}

func TestStorePutGetRoundTrip(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := testPlan("0123456789ab")
			require.NoError(t, s.Put(ctx, p))
			got, err := s.Get(ctx, p.ID)
			require.NoError(t, err)
			require.Equal(t, p, got)
		})
	}
}

func TestStorePutRefusesOverwrite(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := testPlan("0123456789ab")
			require.NoError(t, s.Put(ctx, p))
			p.Output = "other"
			err := s.Put(ctx, p)
			require.ErrorIs(t, err, ErrConflict)
			got, err := s.Get(ctx, p.ID)
			require.NoError(t, err)
			require.Equal(t, "drop", got.Output)
		})
	}
}

func TestStoreRejectsBadIDBeforeAnyS3Call(t *testing.T) {
	api := newFakeS3()
	s := NewS3Store(api, "bucket")
	ctx := context.Background()
	for _, id := range []string{"", "../x", "a/b", "0123456789AB", "0123456789a", "0123456789abc", "0123456789a.", "0123456789ag"} {
		require.Error(t, s.Put(ctx, testPlan(id)), id)
		_, err := s.Get(ctx, id)
		require.Error(t, err, id)
		_, _, err = s.MarkDispatched(ctx, id, 1, "me")
		require.Error(t, err, id)
		require.Error(t, s.MarkDone(ctx, id, "ok"), id)
	}
	require.Zero(t, api.calls, "no S3 call may happen for an invalid id")
}

func TestStoreGetMissing(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			_, err := s.Get(context.Background(), "0123456789ab")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

func TestMarkDispatchedFirstOnce(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := testPlan("0123456789ab")
			require.NoError(t, s.Put(ctx, p))

			got, first, err := s.MarkDispatched(ctx, p.ID, 111, "alice")
			require.NoError(t, err)
			require.True(t, first)
			require.EqualValues(t, 111, got.DispatchedRunID)

			got, first, err = s.MarkDispatched(ctx, p.ID, 222, "bob")
			require.NoError(t, err)
			require.False(t, first)
			require.EqualValues(t, 111, got.DispatchedRunID, "the loser sees the winner's run id")

			got, err = s.Get(ctx, p.ID)
			require.NoError(t, err)
			require.EqualValues(t, 111, got.DispatchedRunID)
		})
	}
}

func TestMarkDispatchedConcurrentExactlyOne(t *testing.T) {
	s := NewS3Store(newFakeS3(), "bucket")
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, testPlan("0123456789ab")))
	var wins int32
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, first, err := s.MarkDispatched(ctx, "0123456789ab", int64(i+1), "x")
			require.NoError(t, err)
			if first {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, 1, wins)
}

func TestMarkDispatchedUnknownPlan(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			_, _, err := s.MarkDispatched(context.Background(), "0123456789ab", 1, "a")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

func TestMarkDispatchedMarkerContents(t *testing.T) {
	api := newFakeS3()
	s := NewS3Store(api, "bucket")
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, testPlan("0123456789ab")))
	_, _, err := s.MarkDispatched(ctx, "0123456789ab", 42, "alice")
	require.NoError(t, err)
	raw := string(api.objs["plans/0123456789ab.dispatched"])
	require.Contains(t, raw, "\"run_id\":42")
	require.Contains(t, raw, "\"approver\":\"alice\"")
	require.True(t, strings.Contains(raw, "\"at\""))
}

func TestMarkDoneIdempotent(t *testing.T) {
	api := newFakeS3()
	s := NewS3Store(api, "bucket")
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, testPlan("0123456789ab")))
	require.NoError(t, s.MarkDone(ctx, "0123456789ab", "succeeded"))
	require.NoError(t, s.MarkDone(ctx, "0123456789ab", "failed"), "second call is a no-op")
	require.Contains(t, string(api.objs["plans/0123456789ab.done"]), "succeeded")
	require.NotContains(t, string(api.objs["plans/0123456789ab.done"]), "failed")
}
