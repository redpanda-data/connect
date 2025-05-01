// Copyright 2024 Redpanda Data, Inc.
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

package dgraph

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestRistrettoCache(t *testing.T) {
	c, err := newRistrettoCache(0, false, nil)
	require.NoError(t, err)

	ctx := t.Context()

	_, err = c.Get(ctx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)

	require.NoError(t, c.Set(ctx, "foo", []byte("1"), nil))

	var res []byte
	require.Eventually(t, func() bool {
		res, err = c.Get(ctx, "foo")
		return err == nil
	}, time.Millisecond*100, time.Millisecond)
	assert.Equal(t, []byte("1"), res)

	assert.NoError(t, c.Delete(ctx, "foo"))

	_, err = c.Get(ctx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)
}

func TestRistrettoCacheWithTTL(t *testing.T) {
	c, err := newRistrettoCache(0, false, nil)
	require.NoError(t, err)

	ctx := t.Context()

	require.NoError(t, c.Set(ctx, "foo", []byte("1"), nil))

	var res []byte
	require.Eventually(t, func() bool {
		res, err = c.Get(ctx, "foo")
		return err == nil
	}, time.Millisecond*100, time.Millisecond)
	assert.Equal(t, []byte("1"), res)

	assert.NoError(t, c.Delete(ctx, "foo"))

	_, err = c.Get(ctx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)

	ttl := time.Millisecond * 200
	require.NoError(t, c.Set(ctx, "foo", []byte("1"), &ttl))

	assert.Eventually(t, func() bool {
		_, err = c.Get(ctx, "foo")
		return err == service.ErrKeyNotFound
	}, time.Second, time.Millisecond*5)
}
