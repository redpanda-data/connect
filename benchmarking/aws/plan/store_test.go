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
	"sort"
	"strconv"
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

func (e apiErr) Error() string               { return e.code }
func (e apiErr) ErrorCode() string           { return e.code }
func (e apiErr) ErrorMessage() string        { return e.code }
func (apiErr) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

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
	if m := aws.ToString(in.IfMatch); m != "" {
		if !exists {
			return nil, apiErr{"NoSuchKey"} // S3 answers 404, not 412, for If-Match on a missing key
		}
		if m != cur {
			return nil, apiErr{"PreconditionFailed"}
		}
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
	if m := aws.ToString(in.IfMatch); m != "" {
		cur, exists := f.etags[key]
		if !exists {
			return nil, apiErr{"NoSuchKey"}
		}
		if m != cur {
			return nil, apiErr{"PreconditionFailed"}
		}
	}
	delete(f.objs, key)
	delete(f.etags, key)
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	prefix := aws.ToString(in.Prefix)
	keys := make([]string, 0, len(f.objs))
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	// Two keys per page, so listing must follow continuation tokens.
	start := 0
	if tok := aws.ToString(in.ContinuationToken); tok != "" {
		start, _ = strconv.Atoi(tok)
	}
	end := min(start+2, len(keys))
	out := &s3.ListObjectsV2Output{}
	for _, k := range keys[start:end] {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
	}
	if end < len(keys) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(strconv.Itoa(end))
	}
	return out, nil
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
		_, err = s.Claim(ctx, id, "me")
		require.Error(t, err, id)
		require.Error(t, s.RecordRun(ctx, id, 1), id)
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

func TestClaimFirstOnce(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := testPlan("0123456789ab")
			require.NoError(t, s.Put(ctx, p))

			first, err := s.Claim(ctx, p.ID, "alice")
			require.NoError(t, err)
			require.True(t, first)
			first, err = s.Claim(ctx, p.ID, "bob")
			require.NoError(t, err)
			require.False(t, first)

			got, err := s.Get(ctx, p.ID)
			require.NoError(t, err)
			require.True(t, got.Claimed, "a claim with no run is visible")
			require.Equal(t, "alice", got.Approver)
			require.Zero(t, got.DispatchedRunID)
		})
	}
}

func TestClaimConcurrentExactlyOne(t *testing.T) {
	s := NewS3Store(newFakeS3(), "bucket")
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, testPlan("0123456789ab")))
	var wins int32
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			first, err := s.Claim(ctx, "0123456789ab", "x")
			require.NoError(t, err)
			if first {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, wins)
}

func TestRecordRun(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := testPlan("0123456789ab")
			require.NoError(t, s.Put(ctx, p))
			first, err := s.Claim(ctx, p.ID, "alice")
			require.NoError(t, err)
			require.True(t, first)

			require.NoError(t, s.RecordRun(ctx, p.ID, 111))
			require.NoError(t, s.RecordRun(ctx, p.ID, 111), "same run id is a no-op")
			require.ErrorIs(t, s.RecordRun(ctx, p.ID, 222), ErrConflict)

			got, err := s.Get(ctx, p.ID)
			require.NoError(t, err)
			require.True(t, got.Claimed)
			require.Equal(t, "alice", got.Approver)
			require.EqualValues(t, 111, got.DispatchedRunID)
		})
	}
}

func TestClaimUnknownPlan(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			_, err := s.Claim(context.Background(), "0123456789ab", "a")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

func TestClaimMarkerContents(t *testing.T) {
	api := newFakeS3()
	s := NewS3Store(api, "bucket")
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, testPlan("0123456789ab")))
	_, err := s.Claim(ctx, "0123456789ab", "alice")
	require.NoError(t, err)
	require.NoError(t, s.RecordRun(ctx, "0123456789ab", 42))
	raw := string(api.objs["plans/0123456789ab.dispatched"])
	require.Contains(t, raw, "\"approver\":\"alice\"")
	require.Contains(t, raw, "\"at\"")
	require.NotContains(t, raw, "run_id")
	require.Contains(t, string(api.objs["plans/0123456789ab.run"]), "\"run_id\":42")
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

func TestRecordRunRejectsNonPositive(t *testing.T) {
	api := newFakeS3()
	s := NewS3Store(api, "bucket")
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, testPlan("0123456789ab")))
	for _, id := range []int64{0, -1} {
		require.Error(t, s.RecordRun(ctx, "0123456789ab", id))
	}
	require.NotContains(t, api.objs, "plans/0123456789ab.run")
}

// noBucketS3 fails every call as S3 does for a bucket that does not exist.
type noBucketS3 struct{ err error }

func (f noBucketS3) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, f.err
}

func (f noBucketS3) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, f.err
}

func (f noBucketS3) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return nil, f.err
}

func (f noBucketS3) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return nil, f.err
}

func TestS3MissingBucketIsErrNoStore(t *testing.T) {
	ctx := t.Context()
	for name, err := range map[string]error{
		"typed":     &s3types.NoSuchBucket{},
		"api error": apiErr{"NoSuchBucket"},
	} {
		t.Run(name, func(t *testing.T) {
			objs := NewS3Objects(noBucketS3{err: err}, "b")
			_, _, gerr := objs.Get(ctx, "k")
			require.ErrorIs(t, gerr, ErrNoStore)
			require.NotErrorIs(t, gerr, ErrNotFound)
			require.ErrorIs(t, objs.Create(ctx, "k", []byte("{}")), ErrNoStore)
			require.ErrorIs(t, objs.Replace(ctx, "k", []byte("{}"), "e"), ErrNoStore)
			require.ErrorIs(t, objs.Delete(ctx, "k", "e"), ErrNoStore)
			_, lerr := objs.List(ctx, "plans/")
			require.ErrorIs(t, lerr, ErrNoStore)

			lock := NewLock(objs)
			_, aerr := lock.Acquire(ctx, "h", time.Hour)
			require.ErrorIs(t, aerr, ErrNoStore)
			require.ErrorIs(t, lock.Release(ctx, "h"), ErrNoStore)
			_, serr := NewStore(objs).Stale(ctx, time.Now(), time.Hour, time.Hour)
			require.ErrorIs(t, serr, ErrNoStore)
		})
	}
	_, _, err := NewS3Objects(noBucketS3{err: apiErr{"AccessDenied"}}, "b").Get(ctx, "k")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNoStore)
}
