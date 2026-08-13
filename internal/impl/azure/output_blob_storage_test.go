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

package azure

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const bsoTestConnString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

func bsoParseTagsConf(t *testing.T, tagsYAML string) (bsoConfig, error) {
	t.Helper()

	conf := fmt.Sprintf(`
storage_connection_string: %s
container: test-container
path: test-blob.txt
%s`, bsoTestConnString, tagsYAML)

	pConf, err := bsoSpec().ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)

	return bsoConfigFromParsed(pConf)
}

func TestBlobStorageOutputTagsConfig(t *testing.T) {
	t.Run("no tags configured", func(t *testing.T) {
		conf, err := bsoParseTagsConf(t, "")
		require.NoError(t, err)
		assert.Empty(t, conf.Tags)
	})

	t.Run("at the tag limit", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("tags:\n")
		for i := range bsoMaxTagsPermitted {
			fmt.Fprintf(&b, "  key%02d: value%02d\n", i, i)
		}

		conf, err := bsoParseTagsConf(t, b.String())
		require.NoError(t, err)
		assert.Len(t, conf.Tags, bsoMaxTagsPermitted)
	})

	t.Run("over the tag limit", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("tags:\n")
		for i := range bsoMaxTagsPermitted + 1 {
			fmt.Fprintf(&b, "  key%02d: value%02d\n", i, i)
		}

		_, err := bsoParseTagsConf(t, b.String())
		require.Error(t, err)
		assert.EqualError(t, err, fmt.Sprintf("at most %d blob index tags are permitted, got %d", bsoMaxTagsPermitted, bsoMaxTagsPermitted+1))
	})

	t.Run("keys are sorted deterministically", func(t *testing.T) {
		// Enough keys that an unsorted implementation is vanishingly unlikely to
		// produce this order by chance via Go's randomised map iteration.
		conf, err := bsoParseTagsConf(t, "tags:\n  hotel: h\n  golf: g\n  foxtrot: f\n  echo: e\n  delta: d\n  charlie: c\n  bravo: b\n  alpha: a\n")
		require.NoError(t, err)

		keys := make([]string, 0, len(conf.Tags))
		for _, pair := range conf.Tags {
			keys = append(keys, pair.key)
		}
		assert.Equal(t, []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}, keys)
	})

	t.Run("invalid keys are rejected at config time", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			key    string
			errStr string
		}{
			{
				name:   "empty key",
				key:    `"": value`,
				errStr: `blob index tag key "" must be between 1 and 128 characters, got 0`,
			},
			{
				name:   "over-long key",
				key:    strings.Repeat("k", bsoMaxTagKeyLength+1) + ": value",
				errStr: fmt.Sprintf("blob index tag key %q must be between 1 and 128 characters, got %d", strings.Repeat("k", bsoMaxTagKeyLength+1), bsoMaxTagKeyLength+1),
			},
			{
				name:   "unsupported character",
				key:    `"bad!key": value`,
				errStr: `blob index tag key "bad!key" contains unsupported character '!', only alphanumerics and the characters " +-./:=_" are permitted`,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				_, err := bsoParseTagsConf(t, "tags:\n  "+test.key+"\n")
				require.Error(t, err)
				assert.EqualError(t, err, test.errStr)
			})
		}
	})

	t.Run("keys at the length limit and using allowed specials are accepted", func(t *testing.T) {
		maxKey := strings.Repeat("k", bsoMaxTagKeyLength)
		conf, err := bsoParseTagsConf(t, fmt.Sprintf("tags:\n  %s: a\n  \"with spaces\": b\n  \"a+b-c.d/e:f=g_h\": c\n", maxKey))
		require.NoError(t, err)
		assert.Len(t, conf.Tags, 3)
	})

	t.Run("values are interpolated per message", func(t *testing.T) {
		conf, err := bsoParseTagsConf(t, "tags:\n  Static: fixed\n  Topic: ${! meta(\"kafka_topic\") }\n")
		require.NoError(t, err)
		require.Len(t, conf.Tags, 2)

		msg := service.NewMessage([]byte("hello world"))
		msg.MetaSetMut("kafka_topic", "orders")

		resolved := make(map[string]string, len(conf.Tags))
		for _, pair := range conf.Tags {
			val, err := pair.value.TryString(msg)
			require.NoError(t, err)
			resolved[pair.key] = val
		}

		assert.Equal(t, map[string]string{
			"Static": "fixed",
			"Topic":  "orders",
		}, resolved)
	})
}
