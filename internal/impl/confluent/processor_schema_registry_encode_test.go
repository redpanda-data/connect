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

package confluent

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var noopReqSign = func(fs.FS, *http.Request) error { return nil }

func TestSchemaRegistryEncoderConfigParse(t *testing.T) {
	configTests := []struct {
		name            string
		config          string
		errContains     string
		expectedBaseURL string
	}{
		{
			name: "bad url",
			config: `
url: huh#%#@$u*not////::example.com
subject: foo
`,
			errContains: `parsing url`,
		},
		{
			name: "bad subject",
			config: `
url: http://example.com
subject: ${! bad interpolation }
`,
			errContains: `failed to parse interpolated field`,
		},
		{
			name: "use default period",
			config: `
url: http://example.com
subject: foo
`,
			expectedBaseURL: "http://example.com",
		},
		{
			name: "bad period",
			config: `
url: http://example.com
subject: foo
refresh_period: not a duration
`,
			errContains: "invalid duration",
		},
		{
			name: "url with base path",
			config: `
url: http://example.com/v1
subject: foo
`,
			expectedBaseURL: "http://example.com/v1",
		},
		{
			name: "url with basic auth",
			config: `
url: http://example.com/v1
basic_auth:
  enabled: true
  username: user
  password: pass
subject: foo
`,
			expectedBaseURL: "http://example.com/v1",
		},
	}

	spec := schemaRegistryEncoderConfig()
	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(test.config, env)
			require.NoError(t, err)

			e, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
			if err == nil {
				_ = e.Close(t.Context())
			}
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestSchemaRegistryEncodeAvro(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchema,
		ID:     3,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo%2Fbar/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo/bar")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  `{"Address":{"my.namespace.com.address":{"City":{"string":"foo"},"State":"bar"}},"Name":"foo","MaybeHobby":{"string":"dancing"}}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x02\x0edancing",
		},
		{
			name:   "successful message null hobby",
			input:  `{"Address":{"my.namespace.com.address":{"City":{"string":"foo"},"State":"bar"}},"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x00",
		},
		{
			name:   "successful message no address and null hobby",
			input:  `{"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x00\x00",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"Address":{"my.namespace.com.address":"not this","Name":"foo"}}`,
			errContains: "cannot decode textual union: cannot decode textual record",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeAvroRawJSON(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchema,
		ID:     3,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":"dancing"}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x02\x0edancing",
		},
		{
			name:   "successful message null hobby",
			input:  `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x00",
		},
		{
			name:   "successful message no address and null hobby",
			input:  `{"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x00\x00",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"Address":{"City":"foo","State":30},"Name":"foo","MaybeHobby":null}`,
			errContains: "could not decode any json data in input",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeAvroLogicalTypes(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchemaLogicalTypes,
		ID:     4,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message with logical types avro json",
			input:  `{"int_time_millis":{"int.time-millis":35245000},"long_time_micros":{"long.time-micros":20192000000000},"long_timestamp_micros":{"long.timestamp-micros":62135596800000000},"pos_0_33333333":{"bytes.decimal":"!"}}`,
			output: "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
		},
		{
			name:        "message doesnt match schema codec",
			input:       `{"int_time_millis":35245000,"long_time_micros":20192000000000,"long_timestamp_micros":null,"pos_0_33333333":"!"}`,
			errContains: "cannot decode textual union: expected:",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"int_time_millis":{"long.time-millis":35245000},"long_time_micros":{"long.time-micros":20192000000000},"long_timestamp_micros":{"long.timestamp-micros":62135596800000000},"pos_0_33333333":{"bytes.decimal":"!"}}`,
			errContains: "cannot decode textual union: cannot decode textual map: cannot determine codec:",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeAvroRawJSONLogicalTypes(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchemaLogicalTypes,
		ID:     4,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message with logical types raw json",
			input:  `{"int_time_millis":35245000,"long_time_micros":20192000000000,"long_timestamp_micros":62135596800000000,"pos_0_33333333":"!"}`,
			output: "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
		},
		{
			name:        "message doesnt match schema codec",
			input:       `{"int_time_millis":{"int.time-millis":35245000},"long_time_micros":{"long.time-micros":20192000000000},"long_timestamp_micros":{"long.timestamp-micros":62135596800000000},"pos_0_33333333":{"bytes.decimal":"!"}}`,
			errContains: "could not decode any json data in input",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"int_time_millis":"35245000","long_time_micros":20192000000000,"long_timestamp_micros":62135596800000000,"pos_0_33333333":"!"}`,
			errContains: "could not decode any json data in input",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeClearExpired(t *testing.T) {
	urlStr := runSchemaRegistryServer(t, func(string) ([]byte, error) {
		return nil, fmt.Errorf("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, encoder.Close(t.Context()))

	tStale := time.Now().Add(-time.Hour).Unix()
	tNotStale := time.Now().Unix()
	tNearlyStale := time.Now().Add(-(schemaStaleAfter / 2)).Unix()

	encoder.cacheMut.Lock()
	encoder.schemas = map[string]cachedSchemaEncoder{
		"5":  {lastUsedUnixSeconds: tStale, lastUpdatedUnixSeconds: tNotStale},
		"10": {lastUsedUnixSeconds: tNotStale, lastUpdatedUnixSeconds: tNotStale},
		"15": {lastUsedUnixSeconds: tNearlyStale, lastUpdatedUnixSeconds: tNotStale},
	}
	encoder.cacheMut.Unlock()

	encoder.refreshEncoders()

	encoder.cacheMut.Lock()
	assert.Equal(t, map[string]cachedSchemaEncoder{
		"10": {lastUsedUnixSeconds: tNotStale, lastUpdatedUnixSeconds: tNotStale},
		"15": {lastUsedUnixSeconds: tNearlyStale, lastUpdatedUnixSeconds: tNotStale},
	}, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeRefresh(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchema,
		ID:     2,
	})
	require.NoError(t, err)

	barFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchema,
		ID:     12,
	})
	require.NoError(t, err)

	var fooReqs, barReqs int32
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/foo/versions/latest":
			atomic.AddInt32(&fooReqs, 1)
			return fooFirst, nil
		case "/subjects/bar/versions/latest":
			atomic.AddInt32(&barReqs, 1)
			return barFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, encoder.Close(t.Context()))

	tStale := time.Now().Add(-time.Hour).Unix()
	tNotStale := time.Now().Unix()
	tNearlyStale := time.Now().Add(-(schemaStaleAfter / 2)).Unix()

	encoder.nowFn = func() time.Time {
		return time.Unix(tNotStale, 0)
	}

	encoder.cacheMut.Lock()
	encoder.schemas = map[string]cachedSchemaEncoder{
		"foo": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tStale,
			id:                     1,
		},
		"bar": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNearlyStale,
			id:                     11,
		},
	}
	encoder.cacheMut.Unlock()

	assert.Equal(t, int32(0), atomic.LoadInt32(&fooReqs))
	assert.Equal(t, int32(0), atomic.LoadInt32(&barReqs))

	encoder.refreshEncoders()

	encoder.cacheMut.Lock()
	tmpFoo := encoder.schemas["foo"]
	tmpFoo.encoder = nil
	encoder.schemas["foo"] = tmpFoo
	assert.Equal(t, map[string]cachedSchemaEncoder{
		"foo": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNotStale,
			id:                     2,
		},
		"bar": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNearlyStale,
			id:                     11,
		},
	}, encoder.schemas)
	tmpBar := encoder.schemas["bar"]
	tmpBar.lastUpdatedUnixSeconds = tStale
	encoder.schemas["bar"] = tmpBar
	encoder.cacheMut.Unlock()

	assert.Equal(t, int32(1), atomic.LoadInt32(&fooReqs))
	assert.Equal(t, int32(0), atomic.LoadInt32(&barReqs))

	encoder.refreshEncoders()

	encoder.cacheMut.Lock()
	tmpBar = encoder.schemas["bar"]
	tmpBar.encoder = nil
	encoder.schemas["bar"] = tmpBar
	assert.Equal(t, map[string]cachedSchemaEncoder{
		"foo": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNotStale,
			id:                     2,
		},
		"bar": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNotStale,
			id:                     12,
		},
	}, encoder.schemas)
	encoder.cacheMut.Unlock()

	assert.Equal(t, int32(1), atomic.LoadInt32(&fooReqs))
	assert.Equal(t, int32(1), atomic.LoadInt32(&barReqs))
}

func TestSchemaRegistryEncodeJSON(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
		ID         int    `json:"id"`
	}{
		Schema:     testJSONSchema,
		SchemaType: "JSON",
		ID:         3,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":"dancing"}`,
			output: "\x00\x00\x00\x00\x03{\"Address\":{\"City\":\"foo\",\"State\":\"bar\"},\"Name\":\"foo\",\"MaybeHobby\":\"dancing\"}",
		},
		{
			name:   "successful message null hobby",
			input:  `{"Address":{"City": "foo","State":"bar"},"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03{\"Address\":{\"City\": \"foo\",\"State\":\"bar\"},\"Name\":\"foo\",\"MaybeHobby\":null}",
		},
		{
			name:   "successful message no address and null hobby",
			input:  `{"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03{\"Name\":\"foo\",\"MaybeHobby\":null}",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"Address":"not this","Name":"foo"}`,
			errContains: "json message does not conform to schema",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeJSONConstantRefreshes(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m != t.Name() {
		t.Skip()
	}

	fooID := int64(1)
	nextFoo := func() []byte {
		t.Helper()
		fooData, err := json.Marshal(struct {
			Schema     string `json:"schema"`
			SchemaType string `json:"schemaType"`
			ID         int64  `json:"id"`
		}{
			Schema:     testJSONSchema,
			SchemaType: "JSON",
			ID:         atomic.AddInt64(&fooID, 1),
		})
		require.NoError(t, err)
		return fooData
	}

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return nextFoo(), nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Millisecond, time.Millisecond*10, service.MockResources())
	require.NoError(t, err)

	input := `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":"dancing"}`
	outputPrefix := "\x00\x00\x00"
	outputSuffix := "{\"Address\":{\"City\":\"foo\",\"State\":\"bar\"},\"Name\":\"foo\",\"MaybeHobby\":\"dancing\"}"

	tStarted := time.Now()

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for time.Since(tStarted) <= (time.Second * 300) {

				outBatches, err := encoder.ProcessBatch(
					t.Context(),
					service.MessageBatch{service.NewMessage([]byte(input))},
				)
				require.NoError(t, err)
				require.Len(t, outBatches, 1)
				require.Len(t, outBatches[0], 1)

				err = outBatches[0][0].GetError()
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				require.True(t, strings.HasPrefix(string(b), outputPrefix), string(b))
				require.True(t, strings.HasSuffix(string(b), outputSuffix), string(b))
			}
		})
	}

	wg.Wait()

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

//------------------------------------------------------------------------------
// Metadata-mode tests
//------------------------------------------------------------------------------

// metaMockRegistration records a single CreateSchema call.
type metaMockRegistration struct {
	Subject   string
	SchemaStr string
	Normalize bool
	ID        int
}

// metaMockState holds all the tracked state from a mock registry.
type metaMockState struct {
	mu            sync.Mutex
	nextID        int
	calls         map[string]int         // subject → count
	registrations []metaMockRegistration // ordered list
	schemas       map[int]string         // id → schema body
	idToSubject   map[int]string         // id → subject (for versions endpoint)
	idToVersion   map[int]int            // id → version within subject
	subjectVer    map[string]int         // subject → next version counter
}

func newMetaMockState() *metaMockState {
	return &metaMockState{
		nextID:      1,
		calls:       map[string]int{},
		schemas:     map[int]string{},
		idToSubject: map[int]string{},
		idToVersion: map[int]int{},
		subjectVer:  map[string]int{},
	}
}

func (s *metaMockState) getCalls() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make(map[string]int, len(s.calls))
	maps.Copy(cp, s.calls)
	return cp
}

func (s *metaMockState) getRegistrations() []metaMockRegistration {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]metaMockRegistration, len(s.registrations))
	copy(cp, s.registrations)
	return cp
}

// runMetaMockRegistry creates a mock schema registry that handles
// POST /subjects/{subject}/versions for CreateSchema, returning incrementing IDs.
// It also handles the franz-go follow-up GET requests for schema validation.
func runMetaMockRegistry(t *testing.T) (url string, state *metaMockState) {
	t.Helper()

	state = newMetaMockState()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		defer state.mu.Unlock()

		path := r.URL.Path

		// POST /subjects/{subject}/versions — CreateSchema
		if r.Method == http.MethodPost && strings.Contains(path, "/subjects/") && strings.HasSuffix(path, "/versions") {
			body, _ := io.ReadAll(r.Body)
			subject := strings.TrimPrefix(path, "/subjects/")
			subject = strings.TrimSuffix(subject, "/versions")
			state.calls[subject]++

			normalize := r.URL.Query().Get("normalize") == "true"

			id := state.nextID
			state.nextID++

			var posted map[string]any
			_ = json.Unmarshal(body, &posted)
			schemaStr, _ := posted["schema"].(string)
			state.schemas[id] = schemaStr
			state.idToSubject[id] = subject

			state.subjectVer[subject]++
			version := state.subjectVer[subject]
			state.idToVersion[id] = version

			state.registrations = append(state.registrations, metaMockRegistration{
				Subject:   subject,
				SchemaStr: schemaStr,
				Normalize: normalize,
				ID:        id,
			})

			resp, _ := json.Marshal(map[string]int{"id": id})
			_, _ = w.Write(resp)
			return
		}

		// GET /schemas/ids/{id}/versions — franz-go calls this after CreateSchema.
		if r.Method == http.MethodGet && strings.HasPrefix(path, "/schemas/ids/") && strings.HasSuffix(path, "/versions") {
			idPart := strings.TrimPrefix(path, "/schemas/ids/")
			idPart = strings.TrimSuffix(idPart, "/versions")
			var id int
			if _, err := fmt.Sscanf(idPart, "%d", &id); err == nil {
				if subject, ok := state.idToSubject[id]; ok {
					resp, _ := json.Marshal([]map[string]any{
						{"subject": subject, "version": state.idToVersion[id]},
					})
					_, _ = w.Write(resp)
					return
				}
			}
		}

		// GET /schemas/ids/{id} — GetSchemaByID
		if r.Method == http.MethodGet && strings.HasPrefix(path, "/schemas/ids/") && !strings.HasSuffix(path, "/versions") {
			idPart := strings.TrimPrefix(path, "/schemas/ids/")
			var id int
			if _, err := fmt.Sscanf(idPart, "%d", &id); err == nil {
				if schemaBody, ok := state.schemas[id]; ok {
					resp, _ := json.Marshal(map[string]any{
						"schema": schemaBody,
						"id":     id,
					})
					_, _ = w.Write(resp)
					return
				}
			}
		}

		// GET /subjects/{subject}/versions/{version} — franz-go fetches this to validate
		if r.Method == http.MethodGet && strings.Contains(path, "/subjects/") && strings.Contains(path, "/versions/") {
			parts := strings.SplitN(strings.TrimPrefix(path, "/subjects/"), "/versions/", 2)
			if len(parts) == 2 {
				var version int
				if _, err := fmt.Sscanf(parts[1], "%d", &version); err == nil {
					// Find the schema ID by subject+version.
					for id, subj := range state.idToSubject {
						if subj == parts[0] && state.idToVersion[id] == version {
							resp, _ := json.Marshal(map[string]any{
								"subject": parts[0],
								"version": version,
								"id":      id,
								"schema":  state.schemas[id],
							})
							_, _ = w.Write(resp)
							return
						}
					}
				}
			}
		}

		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)

	return ts.URL, state
}

func makeCommonSchemaMeta(t *testing.T, fields ...schema.Common) any {
	t.Helper()
	c := schema.Common{
		Type:     schema.Object,
		Name:     "test_record",
		Children: fields,
	}
	return c.ToAny()
}

func TestSchemaRegistryEncodeMetadataAvroHappyPath(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "age", Type: schema.Int32},
	)

	msg := service.NewMessage([]byte(`{"name":"alice","age":30}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)
	require.NoError(t, outBatches[0][0].GetError())

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)

	// Verify Confluent wire format: magic byte + 4-byte schema ID + Avro binary.
	require.Greater(t, len(b), 5, "output must have wire header")
	assert.Equal(t, byte(0x00), b[0], "magic byte")
	schemaID := binary.BigEndian.Uint32(b[1:5])
	assert.Equal(t, uint32(1), schemaID)
	assert.Equal(t, 1, mockState.getCalls()["test-subject"])
}

func TestSchemaRegistryEncodeMetadataMissingMetadata(t *testing.T) {
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	msg := service.NewMessage([]byte(`{"name":"alice"}`))
	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)

	msgErr := outBatches[0][0].GetError()
	require.Error(t, msgErr)
	assert.Contains(t, msgErr.Error(), "schema metadata key")
}

func TestSchemaRegistryEncodeMetadataCaching(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})

	for range 2 {
		msg := service.NewMessage([]byte(`{"x":1}`))
		msg.MetaSetMut("schema", schemaMeta)
		outBatches, bErr := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
		require.NoError(t, bErr)
		require.NoError(t, outBatches[0][0].GetError())
	}

	assert.Equal(t, 1, mockState.getCalls()["test-subject"], "schema should be registered only once")
}

func TestSchemaRegistryEncodeMetadataSchemaEvolution(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemav1 := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})
	msg1 := service.NewMessage([]byte(`{"x":1}`))
	msg1.MetaSetMut("schema", schemav1)
	out1, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg1})
	require.NoError(t, err)
	require.NoError(t, out1[0][0].GetError())

	schemav2 := makeCommonSchemaMeta(t,
		schema.Common{Name: "x", Type: schema.Int32},
		schema.Common{Name: "y", Type: schema.String},
	)
	msg2 := service.NewMessage([]byte(`{"x":1,"y":"hello"}`))
	msg2.MetaSetMut("schema", schemav2)
	out2, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg2})
	require.NoError(t, err)
	require.NoError(t, out2[0][0].GetError())

	assert.Equal(t, 2, mockState.getCalls()["test-subject"])

	b1, _ := out1[0][0].AsBytes()
	b2, _ := out2[0][0].AsBytes()
	id1 := binary.BigEndian.Uint32(b1[1:5])
	id2 := binary.BigEndian.Uint32(b2[1:5])
	assert.NotEqual(t, id1, id2, "different schemas should get different IDs")
}

func TestSchemaRegistryEncodeMetadataRegistryError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, ts.URL), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)

	msgErr := outBatches[0][0].GetError()
	require.Error(t, msgErr)
	assert.Contains(t, msgErr.Error(), "registering schema")
}

func TestSchemaRegistryEncodeMetadataJSONSchemaHappyPath(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: json_schema
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "age", Type: schema.Int32},
	)
	msg := service.NewMessage([]byte(`{"name":"alice","age":30}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)

	require.Greater(t, len(b), 5)
	assert.Equal(t, byte(0x00), b[0])
	assert.Equal(t, `{"name":"alice","age":30}`, string(b[5:]))
	assert.Equal(t, 1, mockState.getCalls()["test-subject"])
}

func TestSchemaRegistryEncodeMetadataJSONSchemaValidationFailure(t *testing.T) {
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: json_schema
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "age", Type: schema.Int32},
	)
	msg := service.NewMessage([]byte(`{"name":"alice","age":"not a number"}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)

	msgErr := outBatches[0][0].GetError()
	require.Error(t, msgErr)
	assert.Contains(t, msgErr.Error(), "does not conform to schema")
}

func TestSchemaRegistryEncodeMetadataConfigValidation(t *testing.T) {
	spec := schemaRegistryEncoderConfig()
	env := service.NewEnvironment()

	tests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "schema_metadata without format",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
`,
			errContains: "format is required",
		},
		{
			name: "format without schema_metadata",
			config: `
url: http://example.com
subject: foo
format: avro
`,
			errContains: "format is only used when schema_metadata is set",
		},
		{
			name: "avro format without explicit raw_json",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: avro
`,
			errContains: "avro.raw_json to be explicitly set",
		},
		{
			name: "avro format with avro.raw_json succeeds",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: avro
avro:
  raw_json: true
`,
		},
		{
			name: "avro format with deprecated avro_raw_json still requires avro.raw_json",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: avro
avro_raw_json: true
`,
			errContains: "avro.raw_json to be explicitly set",
		},
		{
			name: "json_schema format without raw_json succeeds",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: json_schema
`,
		},
		{
			name: "avro.raw_json overrides avro_raw_json",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: avro
avro_raw_json: false
avro:
  raw_json: true
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(test.config, env)
			require.NoError(t, err)

			e, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
			if e != nil {
				_ = e.Close(t.Context())
			}
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

//------------------------------------------------------------------------------
// Additional metadata-mode coverage
//------------------------------------------------------------------------------

func TestSchemaRegistryEncodeMetadataAvroJSONEncoding(t *testing.T) {
	// Test with avro.raw_json: false — messages must use Avro JSON union format.
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: false
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "hobby", Type: schema.String, Optional: true},
	)

	// Avro JSON format: optional fields require {"string": "value"} wrapper.
	msg := service.NewMessage([]byte(`{"name":"alice","hobby":{"string":"dancing"}}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)
	require.Greater(t, len(b), 5, "output must have wire header + avro binary")

	// Verify null hobby also works in Avro JSON format.
	msg2 := service.NewMessage([]byte(`{"name":"bob","hobby":null}`))
	msg2.MetaSetMut("schema", schemaMeta)
	out2, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg2})
	require.NoError(t, err)
	require.NoError(t, out2[0][0].GetError())

	_ = mockState
}

func TestSchemaRegistryEncodeMetadataRecordNameAndNamespace(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
  record_name: CustomRecord
  namespace: com.example.test
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	// Use a schema with no root name so the configured record_name is used.
	c := schema.Common{
		Type:     schema.Object,
		Children: []schema.Common{{Name: "x", Type: schema.Int32}},
	}
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", c.ToAny())

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	regs := mockState.getRegistrations()
	require.Len(t, regs, 1)

	var avroSchema map[string]any
	require.NoError(t, json.Unmarshal([]byte(regs[0].SchemaStr), &avroSchema))
	assert.Equal(t, "CustomRecord", avroSchema["name"])
	assert.Equal(t, "com.example.test", avroSchema["namespace"])
}

func TestSchemaRegistryEncodeMetadataRecordNameFromSubject(t *testing.T) {
	// When record_name is not set and Common.Name is empty, derive from subject.
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: my-topic-value
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	// Schema with no root name — subject should be used as fallback.
	c := schema.Common{
		Type:     schema.Object,
		Children: []schema.Common{{Name: "x", Type: schema.Int32}},
	}
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", c.ToAny())

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	regs := mockState.getRegistrations()
	require.Len(t, regs, 1)

	var avroSchema map[string]any
	require.NoError(t, json.Unmarshal([]byte(regs[0].SchemaStr), &avroSchema))
	assert.Equal(t, "my_topic_value", avroSchema["name"], "hyphens should be sanitized to underscores")
}

func TestSchemaRegistryEncodeMetadataSubjectInterpolation(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: ${! meta("kafka_topic") }-value
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})

	// Two messages with different topics → different subjects → separate registrations.
	msg1 := service.NewMessage([]byte(`{"x":1}`))
	msg1.MetaSetMut("kafka_topic", "topicA")
	msg1.MetaSetMut("schema", schemaMeta)

	msg2 := service.NewMessage([]byte(`{"x":2}`))
	msg2.MetaSetMut("kafka_topic", "topicB")
	msg2.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg1, msg2})
	require.NoError(t, err)
	require.Len(t, outBatches[0], 2)
	require.NoError(t, outBatches[0][0].GetError())
	require.NoError(t, outBatches[0][1].GetError())

	calls := mockState.getCalls()
	assert.Equal(t, 1, calls["topicA-value"])
	assert.Equal(t, 1, calls["topicB-value"])
}

func TestSchemaRegistryEncodeMetadataMixedBatch(t *testing.T) {
	// A batch where one message has schema metadata and another doesn't.
	// The invalid message should get an error; the valid one should succeed.
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})

	good := service.NewMessage([]byte(`{"x":1}`))
	good.MetaSetMut("schema", schemaMeta)

	bad := service.NewMessage([]byte(`{"x":2}`))
	// bad has no schema metadata

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{good, bad})
	require.NoError(t, err)
	require.Len(t, outBatches[0], 2)

	require.NoError(t, outBatches[0][0].GetError(), "good message should succeed")

	badErr := outBatches[0][1].GetError()
	require.Error(t, badErr, "bad message should have error")
	assert.Contains(t, badErr.Error(), "schema metadata key")
}

func TestSchemaRegistryEncodeMetadataNormalize(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
normalize: true
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	regs := mockState.getRegistrations()
	require.Len(t, regs, 1)
	assert.True(t, regs[0].Normalize, "normalize should be true in the CreateSchema request")
}

func TestExtractFingerprint(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		meta := map[string]any{"fingerprint": "abc123", "type": "OBJECT"}
		fp, err := extractFingerprint(meta)
		require.NoError(t, err)
		assert.Equal(t, "abc123", fp)
	})

	t.Run("not a map", func(t *testing.T) {
		_, err := extractFingerprint("not a map")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected map[string]any")
	})

	t.Run("missing fingerprint", func(t *testing.T) {
		meta := map[string]any{"type": "OBJECT"}
		_, err := extractFingerprint(meta)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing or invalid fingerprint")
	})

	t.Run("fingerprint wrong type", func(t *testing.T) {
		meta := map[string]any{"fingerprint": 12345}
		_, err := extractFingerprint(meta)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing or invalid fingerprint")
	})
}

func TestSchemaRegistryEncodeMetadataPurgeStale(t *testing.T) {
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	// Encode a message to populate the metaEncoders cache.
	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	// Verify cache has an entry.
	encoder.metaCacheMut.RLock()
	assert.Len(t, encoder.metaEncoders, 1)
	encoder.metaCacheMut.RUnlock()

	// Manually set lastUsedUnixSeconds to a stale time.
	tStale := time.Now().Add(-time.Hour).Unix()
	encoder.metaCacheMut.Lock()
	for k, v := range encoder.metaEncoders {
		v.lastUsedUnixSeconds = tStale
		encoder.metaEncoders[k] = v
	}
	encoder.metaCacheMut.Unlock()

	// Run purge.
	encoder.purgeStaleMetaEncoders()

	// Cache should now be empty.
	encoder.metaCacheMut.RLock()
	assert.Empty(t, encoder.metaEncoders, "stale entries should be purged")
	encoder.metaCacheMut.RUnlock()
}

func TestSchemaRegistryEncodeMetadataConcurrent(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "x", Type: schema.Int32},
	)

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 50 {
				msg := service.NewMessage([]byte(`{"x":42}`))
				msg.MetaSetMut("schema", schemaMeta)

				outBatches, bErr := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
				if bErr != nil {
					t.Errorf("ProcessBatch error: %v", bErr)
					return
				}
				if msgErr := outBatches[0][0].GetError(); msgErr != nil {
					t.Errorf("message error: %v", msgErr)
					return
				}

				b, bErr := outBatches[0][0].AsBytes()
				if bErr != nil {
					t.Errorf("AsBytes error: %v", bErr)
					return
				}
				if len(b) <= 5 {
					t.Errorf("output too short: %d bytes", len(b))
					return
				}
			}
		})
	}
	wg.Wait()

	// Despite 500 total calls, schema should only be registered once.
	assert.Equal(t, 1, mockState.getCalls()["test-subject"])
}

func TestSchemaRegistryEncodeMetadataAvroTimestamp(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: products-value
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	// Simulate the exact schema a CDC source would produce for a table with
	// a TIMESTAMPTZ column.
	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "id", Type: schema.Int32},
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "price", Type: schema.String},
		schema.Common{Name: "in_stock", Type: schema.Boolean},
		schema.Common{Name: "created_at", Type: schema.Timestamp, Optional: true},
	)

	msg := service.NewMessage([]byte(`{"id":79,"name":"budget gadget","price":"79.06","in_stock":true,"created_at":"2026-03-19T10:05:09.934345Z"}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)
	require.NoError(t, outBatches[0][0].GetError(), "encoding a CDC message with a timestamp field should succeed")

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)

	// Verify Confluent wire format header.
	require.Greater(t, len(b), 5, "output must have wire header")
	assert.Equal(t, byte(0x00), b[0], "magic byte")
	schemaID := binary.BigEndian.Uint32(b[1:5])
	assert.Equal(t, uint32(1), schemaID)
	assert.Equal(t, 1, mockState.getCalls()["products-value"])
}
