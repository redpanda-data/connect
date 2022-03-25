package confluent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

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
			errContains: `failed to parse url`,
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
	}

	spec := schemaRegistryEncoderConfig()
	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(test.config, env)
			require.NoError(t, err)

			e, err := newSchemaRegistryEncoderFromConfig(conf, nil)

			if e != nil {
				assert.Equal(t, test.expectedBaseURL, e.schemaRegistryBaseURL.String())
			}

			if err == nil {
				_ = e.Close(context.Background())
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

	encoder, err := newSchemaRegistryEncoder(urlStr, nil, subj, true, time.Minute*10, time.Minute, nil)
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
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar\x02\x0edancing",
		},
		{
			name:   "successful message null hobby",
			input:  `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar\x00",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"Address":{"City":"foo","State":30},"Name":"foo","MaybeHobby":null}`,
			errContains: "could not decode any json data in input",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				context.Background(),
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

	require.NoError(t, encoder.Close(context.Background()))
	encoder.cacheMut.Lock()
	assert.Len(t, encoder.schemas, 0)
	encoder.cacheMut.Unlock()
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
		if path == "/subjects/foo/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, nil, subj, false, time.Minute*10, time.Minute, nil)
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  `{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo","MaybeHobby":{"string":"dancing"}}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar\x02\x0edancing",
		},
		{
			name:   "successful message null hobby",
			input:  `{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar\x00",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"Address":{"my.namespace.com.address":"not this","Name":"foo"}}`,
			errContains: "schema does not specify default value",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				context.Background(),
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

	require.NoError(t, encoder.Close(context.Background()))
	encoder.cacheMut.Lock()
	assert.Len(t, encoder.schemas, 0)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeClearExpired(t *testing.T) {
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		return nil, fmt.Errorf("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, nil, subj, false, time.Minute*10, time.Minute, nil)
	require.NoError(t, err)
	require.NoError(t, encoder.Close(context.Background()))

	tStale := time.Now().Add(-time.Hour).Unix()
	tNotStale := time.Now().Unix()
	tNearlyStale := time.Now().Add(-(schemaStaleAfter / 2)).Unix()

	encoder.cacheMut.Lock()
	encoder.schemas = map[string]*cachedSchemaEncoder{
		"5":  {lastUsedUnixSeconds: tStale, lastUpdatedUnixSeconds: tNotStale},
		"10": {lastUsedUnixSeconds: tNotStale, lastUpdatedUnixSeconds: tNotStale},
		"15": {lastUsedUnixSeconds: tNearlyStale, lastUpdatedUnixSeconds: tNotStale},
	}
	encoder.cacheMut.Unlock()

	encoder.refreshEncoders()

	encoder.cacheMut.Lock()
	assert.Equal(t, map[string]*cachedSchemaEncoder{
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

	encoder, err := newSchemaRegistryEncoder(urlStr, nil, subj, false, time.Minute*10, time.Minute, nil)
	require.NoError(t, err)
	require.NoError(t, encoder.Close(context.Background()))

	tStale := time.Now().Add(-time.Hour).Unix()
	tNotStale := time.Now().Unix()
	tNearlyStale := time.Now().Add(-(schemaStaleAfter / 2)).Unix()

	encoder.nowFn = func() time.Time {
		return time.Unix(tNotStale, 0)
	}

	encoder.cacheMut.Lock()
	encoder.schemas = map[string]*cachedSchemaEncoder{
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
	encoder.schemas["foo"].encoder = nil
	assert.Equal(t, map[string]*cachedSchemaEncoder{
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
	encoder.schemas["bar"].lastUpdatedUnixSeconds = tStale
	encoder.cacheMut.Unlock()

	assert.Equal(t, int32(1), atomic.LoadInt32(&fooReqs))
	assert.Equal(t, int32(0), atomic.LoadInt32(&barReqs))

	encoder.refreshEncoders()

	encoder.cacheMut.Lock()
	encoder.schemas["bar"].encoder = nil
	assert.Equal(t, map[string]*cachedSchemaEncoder{
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
