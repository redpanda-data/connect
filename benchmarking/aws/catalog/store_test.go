// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeS3 is an in-memory S3API. pageSize > 0 paginates ListObjectsV2.
type fakeS3 struct {
	mu       sync.Mutex
	objs     map[string][]byte
	pageSize int
	puts     int
	// dropToken drops NextContinuationToken from truncated pages.
	dropToken bool
}

func newFakeS3(pageSize int) *fakeS3 {
	return &fakeS3{objs: map[string][]byte{}, pageSize: pageSize}
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[aws.ToString(in.Key)] = b
	f.puts++
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objs[aws.ToString(in.Key)]
	if !ok {
		return nil, os.ErrNotExist
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b))}, nil
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	start := 0
	if tok := aws.ToString(in.ContinuationToken); tok != "" {
		start, _ = strconv.Atoi(tok)
	}
	end := len(keys)
	out := &s3.ListObjectsV2Output{}
	if f.pageSize > 0 && start+f.pageSize < end {
		end = start + f.pageSize
		out.IsTruncated = aws.Bool(true)
		if !f.dropToken {
			out.NextContinuationToken = aws.String(strconv.Itoa(end))
		}
	}
	for _, k := range keys[start:end] {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
	}
	return out, nil
}

type storeFixture struct {
	name string
	// make returns the store, a raw writer that bypasses Put (for planting
	// objects Put would refuse), and a count of objects written so far.
	make func(t *testing.T) (st Store, raw func(key string, data []byte), count func() int)
}

func storeFixtures() []storeFixture {
	return []storeFixture{
		{"dir", func(t *testing.T) (Store, func(string, []byte), func() int) {
			root := t.TempDir()
			raw := func(key string, data []byte) {
				p := filepath.Join(root, filepath.FromSlash(key))
				require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
				require.NoError(t, os.WriteFile(p, data, 0o644))
			}
			count := func() int {
				n := 0
				require.NoError(t, filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
					if err == nil && !d.IsDir() {
						n++
					}
					return err
				}))
				return n
			}
			return NewDirStore(root), raw, count
		}},
		{"s3", func(*testing.T) (Store, func(string, []byte), func() int) {
			f := newFakeS3(0)
			raw := func(key string, data []byte) { f.objs[key] = data }
			return NewS3Store(f, "bkt"), raw, func() int { return len(f.objs) }
		}},
	}
}

func entryWith(id, input string) Entry {
	e := validEntry()
	e.ID = id
	e.Input = input
	return e
}

func ids(es []Entry) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.ID)
	}
	sort.Strings(out)
	return out
}

func TestStoreKey(t *testing.T) {
	require.Equal(t, "catalog/v1/postgres_cdc/sess-1.json", Key(entryWith("sess-1", "postgres_cdc")))
}

func TestStorePutList(t *testing.T) {
	for _, fx := range storeFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			ctx := context.Background()
			st, _, _ := fx.make(t)
			for _, e := range []Entry{
				entryWith("a", "postgres_cdc"),
				entryWith("b", "postgres_cdc"),
				entryWith("c", "mysql_cdc"),
			} {
				require.NoError(t, st.Put(ctx, e))
			}

			pg, err := st.List(ctx, "postgres_cdc")
			require.NoError(t, err)
			assert.Equal(t, []string{"a", "b"}, ids(pg))

			all, err := st.List(ctx, "")
			require.NoError(t, err)
			assert.Equal(t, []string{"a", "b", "c"}, ids(all))

			// Round trip preserves content.
			want := entryWith("a", "postgres_cdc")
			var got Entry
			for _, e := range pg {
				if e.ID == "a" {
					got = e
				}
			}
			assert.Equal(t, want.Points, got.Points)
			assert.True(t, want.RecordedAt.Equal(got.RecordedAt))

			none, err := st.List(ctx, "nope")
			require.NoError(t, err)
			assert.Empty(t, none)
		})
	}
}

func TestStorePutRejectsInvalid(t *testing.T) {
	for _, fx := range storeFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			st, _, count := fx.make(t)
			bad := entryWith("a", "postgres_cdc")
			bad.Fingerprint = ""
			require.ErrorContains(t, st.Put(context.Background(), bad), "fingerprint")

			// Path separators in key parts would escape the prefix.
			require.Error(t, st.Put(context.Background(), entryWith("../x", "postgres_cdc")))
			require.Error(t, st.Put(context.Background(), entryWith("a", "a/b")))
			assert.Zero(t, count())
		})
	}
}

func TestStoreListSkipsUnknownSchema(t *testing.T) {
	for _, fx := range storeFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			ctx := context.Background()
			st, raw, _ := fx.make(t)
			require.NoError(t, st.Put(ctx, entryWith("a", "postgres_cdc")))
			raw("catalog/v1/postgres_cdc/future.json", []byte(`{"schema_version":2,"id":"future","new_field":[1]}`))

			got, err := st.List(ctx, "postgres_cdc")
			require.NoError(t, err)
			assert.Equal(t, []string{"a"}, ids(got))
		})
	}
}

func TestStoreListCorruptIsError(t *testing.T) {
	for _, fx := range storeFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			st, raw, _ := fx.make(t)
			raw("catalog/v1/postgres_cdc/bad.json", []byte(`{not json`))
			_, err := st.List(context.Background(), "postgres_cdc")
			require.ErrorContains(t, err, "bad.json")
		})
	}
}

func TestStoreListPaginates(t *testing.T) {
	ctx := context.Background()
	f := newFakeS3(2)
	st := NewS3Store(f, "bkt")
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, st.Put(ctx, entryWith(id, "postgres_cdc")))
	}
	got, err := st.List(ctx, "postgres_cdc")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, ids(got))
}

func TestStoreListTruncatedWithoutTokenIsError(t *testing.T) {
	ctx := context.Background()
	f := newFakeS3(2)
	st := NewS3Store(f, "bkt")
	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, st.Put(ctx, entryWith(id, "postgres_cdc")))
	}
	f.dropToken = true
	_, err := st.List(ctx, "postgres_cdc")
	require.ErrorContains(t, err, "continuation token")
}

func TestStoreListRejectsBadInput(t *testing.T) {
	for _, fx := range storeFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			st, _, _ := fx.make(t)
			for _, in := range []string{".", "..", "a/b", `a\b`, "../postgres_cdc"} {
				_, err := st.List(context.Background(), in)
				require.ErrorContains(t, err, "input", in)
			}
		})
	}
}
