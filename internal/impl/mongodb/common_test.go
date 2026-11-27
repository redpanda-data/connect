// Copyright 2026 Redpanda Data, Inc.
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

package mongodb

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestIsConnPoolError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "handshake auth failure",
			err:  errors.New(`connection(cluster0-shard-00-01.mongodb.net:27017[-12]) error occurred during connection handshake: auth error: sasl conversation error: unable to authenticate using mechanism "MONGODB-AWS": (AuthenticationFailed) Authentication failed.`),
			want: true,
		},
		{
			name: "pool cleared",
			err:  errors.New("connection pool for cluster0-shard-00-01.mongodb.net:27017 was cleared because another operation failed with: connection() error occurred during connection handshake: auth error"),
			want: true,
		},
		{
			name: "server selection",
			err:  errors.New("server selection error: context deadline exceeded, current topology: { ... }"),
			want: true,
		},
		{
			name: "network error label",
			err:  mongo.CommandError{Labels: []string{"NetworkError"}, Message: "socket was unexpectedly closed"},
			want: true,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "duplicate key",
			err:  mongo.CommandError{Code: 11000, Message: "E11000 duplicate key error collection: foo.bar"},
			want: false,
		},
		{
			name: "validation failure",
			err:  errors.New("document validation failure"),
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, isConnPoolError(test.err))
		})
	}
}

func parseInputConf(t *testing.T, conf string) *service.ParsedConfig {
	t.Helper()
	parsed, err := mongoConfigSpec().ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)
	return parsed
}

const inputConfBoilerplate = `
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
`

func TestAWSAuthRejectsStaticCredentials(t *testing.T) {
	parsed := parseInputConf(t, inputConfBoilerplate+`
username: foouser
password: foopass
aws:
  enabled: true
`)
	_, err := newMongoInput(parsed, service.MockResources().Logger())
	require.ErrorContains(t, err, "username and password cannot be set when aws.enabled is true")
}

func TestAWSAuthNotImportedError(t *testing.T) {
	parsed := parseInputConf(t, inputConfBoilerplate+`
aws:
  enabled: true
`)
	_, err := newMongoInput(parsed, service.MockResources().Logger())
	require.ErrorContains(t, err, "does not import components/aws")
}

func TestAWSAuthRejectsURLCredentials(t *testing.T) {
	parsed := parseInputConf(t, `
url: "mongodb://foouser:foopass@localhost:27017"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
aws:
  enabled: true
`)
	_, err := newMongoInput(parsed, service.MockResources().Logger())
	require.ErrorContains(t, err, "credentials embedded in the url cannot be combined with aws.enabled")
}

func TestAWSAuthAllowsAuthMechanismInURL(t *testing.T) {
	// A url which only names the mechanism - the shape Atlas suggests for IAM
	// users - carries no credentials and must be accepted. Stand in for the aws
	// bundle so construction can complete without importing it here.
	prevOptFn := AWSOptFn
	t.Cleanup(func() { AWSOptFn = prevOptFn })
	AWSOptFn = func(*service.ParsedConfig, *service.Logger) (CredentialBuilder, error) {
		return func(context.Context) (*options.Credential, error) {
			return &options.Credential{AuthMechanism: "MONGODB-AWS"}, nil
		}, nil
	}

	parsed := parseInputConf(t, `
url: "mongodb://localhost:27017/?authSource=%24external&authMechanism=MONGODB-AWS"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
aws:
  enabled: true
`)
	input, err := newMongoInput(parsed, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, input.Close(t.Context()))
}

func TestURIHasUserInfo(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want bool
	}{
		{
			name: "plain with credentials",
			uri:  "mongodb://foouser:foopass@localhost:27017",
			want: true,
		},
		{
			name: "plain without credentials",
			uri:  "mongodb://localhost:27017",
			want: false,
		},
		{
			name: "srv with credentials",
			uri:  "mongodb+srv://foouser:foopass@cluster0.example.invalid/",
			want: true,
		},
		{
			name: "srv naming the mechanism only",
			uri:  "mongodb+srv://cluster0.example.invalid/?authSource=%24external&authMechanism=MONGODB-AWS",
			want: false,
		},
		{
			name: "multi host with credentials",
			uri:  "mongodb://u:p@h1:27017,h2:27018/db",
			want: true,
		},
		{
			name: "multi host without credentials",
			uri:  "mongodb://h1:27017,h2:27018/db",
			want: false,
		},
		{
			name: "escaped at sign in password",
			uri:  "mongodb://foouser:foo%40pass@localhost:27017",
			want: true,
		},
		{
			name: "at sign only in the query string",
			uri:  "mongodb://localhost:27017/db?appName=a@b",
			want: false,
		},
		{
			name: "not a uri",
			uri:  "not-a-mongo-uri",
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, uriHasUserInfo(test.uri))
		})
	}
}

func TestAWSAuthRejectsSRVURLCredentials(t *testing.T) {
	stubAWSOptFn(t)
	// The host is deliberately unresolvable (`.invalid` is a reserved TLD), so
	// this only passes if the rejection is textual rather than driver-parsed.
	parsed := parseInputConf(t, `
url: "mongodb+srv://foouser:foopass@cluster0.example.invalid/"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
aws:
  enabled: true
`)
	_, err := newMongoInput(parsed, service.MockResources().Logger())
	require.ErrorContains(t, err, "credentials embedded in the url cannot be combined with aws.enabled")
}

func TestAWSAuthSRVURLConstructsWithoutDNS(t *testing.T) {
	stubAWSOptFn(t)
	// `cluster0.example.invalid` cannot resolve, so construction can only
	// succeed if no SRV/TXT lookup is attempted: the driver's URI parser
	// resolves those eagerly, so the probe must be skipped for +srv URLs and
	// parse/DNS errors deferred to Connect, which is retried.
	parsed := parseInputConf(t, `
url: "mongodb+srv://cluster0.example.invalid/?authSource=%24external&authMechanism=MONGODB-AWS"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
aws:
  enabled: true
`)
	input, err := newMongoInput(parsed, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, input.Close(t.Context()))
}

func TestInvalidURLRejected(t *testing.T) {
	parsed := parseInputConf(t, `
url: "not-a-mongo-uri"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
`)
	_, err := newMongoInput(parsed, service.MockResources().Logger())
	require.ErrorContains(t, err, "invalid url")
}

func TestAWSAuthDisabledKeepsStaticCredentials(t *testing.T) {
	parsed := parseInputConf(t, inputConfBoilerplate+`
username: foouser
password: foopass
aws:
  enabled: false
`)
	input, err := newMongoInput(parsed, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, input.Close(t.Context()))
}

// stubAWSOptFn swaps in a no-op AWSOptFn for the duration of the test, since
// the mongodb package does not import the aws subpackage and would otherwise
// hit the "does not import components/aws" stub error before the checks under
// test get a chance to run.
func stubAWSOptFn(t *testing.T) {
	t.Helper()
	prev := AWSOptFn
	t.Cleanup(func() { AWSOptFn = prev })
	AWSOptFn = func(*service.ParsedConfig, *service.Logger) (CredentialBuilder, error) {
		return func(context.Context) (*options.Credential, error) {
			return &options.Credential{AuthMechanism: "MONGODB-AWS"}, nil
		}, nil
	}
}

const processorConfBoilerplate = `
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
operation: find-one
filter_map: |
  root.a = this.a
`

func TestAWSAuthProcessorRejectsRoleAssumption(t *testing.T) {
	stubAWSOptFn(t)
	conf, err := ProcessorSpec().ParseYAML(processorConfBoilerplate+`
aws:
  enabled: true
  role: arn:aws:iam::123456789012:role/foo
`, service.NewEnvironment())
	require.NoError(t, err)

	_, err = ProcessorFromParsed(conf, service.MockResources())
	require.ErrorContains(t, err, "cannot be used with the mongodb processor")
}

func TestAWSAuthCacheRejectsRoleAssumption(t *testing.T) {
	stubAWSOptFn(t)
	conf, err := mongodbCacheConfig().ParseYAML(`
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
key_field: "k"
value_field: "v"
aws:
  enabled: true
  role: arn:aws:iam::123456789012:role/foo
`, service.NewEnvironment())
	require.NoError(t, err)

	_, err = newMongodbCacheFromConfig(conf, service.MockResources().Logger())
	require.ErrorContains(t, err, "cannot be used with the mongodb cache")
}

func TestAWSAuthCacheAcceptsAmbientCredentials(t *testing.T) {
	stubAWSOptFn(t)
	conf, err := mongodbCacheConfig().ParseYAML(`
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
key_field: "k"
value_field: "v"
aws:
  enabled: true
`, service.NewEnvironment())
	require.NoError(t, err)

	cache, err := newMongodbCacheFromConfig(conf, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, cache.Close(t.Context()))
}

func TestAWSAuthProcessorRejectsSessionToken(t *testing.T) {
	stubAWSOptFn(t)
	conf, err := ProcessorSpec().ParseYAML(processorConfBoilerplate+`
aws:
  enabled: true
  id: AKIAIOSFODNN7EXAMPLE
  secret: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  token: FwoGZXIvYXdzEXAMPLESESSIONTOKEN
`, service.NewEnvironment())
	require.NoError(t, err)

	_, err = ProcessorFromParsed(conf, service.MockResources())
	require.ErrorContains(t, err, "cannot be used with the mongodb processor")
}

func TestAWSAuthCacheRejectsSessionToken(t *testing.T) {
	stubAWSOptFn(t)
	conf, err := mongodbCacheConfig().ParseYAML(`
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
key_field: "k"
value_field: "v"
aws:
  enabled: true
  id: AKIAIOSFODNN7EXAMPLE
  secret: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  token: FwoGZXIvYXdzEXAMPLESESSIONTOKEN
`, service.NewEnvironment())
	require.NoError(t, err)

	_, err = newMongodbCacheFromConfig(conf, service.MockResources().Logger())
	require.ErrorContains(t, err, "cannot be used with the mongodb cache")
}

func TestAWSAuthCacheAcceptsLongLivedKeys(t *testing.T) {
	stubAWSOptFn(t)
	// Access keys without a session token do not expire, so the cache's lack of
	// a refresh path is not a problem and construction must be allowed.
	conf, err := mongodbCacheConfig().ParseYAML(`
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
key_field: "k"
value_field: "v"
aws:
  enabled: true
  id: AKIAIOSFODNN7EXAMPLE
  secret: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
`, service.NewEnvironment())
	require.NoError(t, err)

	cache, err := newMongodbCacheFromConfig(conf, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, cache.Close(t.Context()))
}

func TestAWSAuthInputAcceptsRoleAssumption(t *testing.T) {
	stubAWSOptFn(t)
	parsed := parseInputConf(t, inputConfBoilerplate+`
aws:
  enabled: true
  role: arn:aws:iam::123456789012:role/foo
`)
	input, err := newMongoInput(parsed, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, input.Close(t.Context()))
}

func TestExtJSONFromMapDontPreserveBinary(t *testing.T) {
	rawBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	base64String := base64.StdEncoding.EncodeToString(rawBytes)

	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{
		"binary_field": rawBytes,
	})

	executor, err := bloblang.Parse("root = this")
	require.NoError(t, err)

	batch := service.MessageBatch{msg}
	mappingExec := batch.BloblangExecutor(executor)

	result, err := extJSONFromMap(0, mappingExec, false)
	require.NoError(t, err)
	require.NotNil(t, result)

	resultD, ok := result.(bson.D)
	require.True(t, ok)

	var fieldValue any
	found := false
	for _, elem := range resultD {
		if elem.Key == "binary_field" {
			fieldValue = elem.Value
			found = true
			break
		}
	}
	require.True(t, found)

	_, isBytes := fieldValue.([]byte)
	assert.False(t, isBytes)

	extractedString, isString := fieldValue.(string)
	assert.True(t, isString)
	assert.Equal(t, base64String, extractedString)
}

func TestExtJSONFromMapPreserveBinary(t *testing.T) {
	rawBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{
		"binary_field": rawBytes,
	})

	executor, err := bloblang.Parse("root = this")
	require.NoError(t, err)

	batch := service.MessageBatch{msg}
	mappingExec := batch.BloblangExecutor(executor)

	result, err := extJSONFromMap(0, mappingExec, true)
	require.NoError(t, err)
	require.NotNil(t, result)

	resultD, ok := result.(bson.D)
	require.True(t, ok)

	var fieldValue any
	found := false
	for _, elem := range resultD {
		if elem.Key == "binary_field" {
			fieldValue = elem.Value
			found = true
			break
		}
	}
	require.True(t, found)

	extractedBytes, isBytes := fieldValue.([]byte)
	assert.True(t, isBytes)
	assert.Equal(t, rawBytes, extractedBytes)
}

func TestProcessExtJSON(t *testing.T) {
	rawBytes := []byte("hello redpanda")
	expectedBase64 := "aGVsbG8gcmVkcGFuZGE="

	tests := []struct {
		name           string
		preserveBinary bool
		input          any
		validate       func(t *testing.T, result any)
	}{
		{
			name:           "Default mode (preserve_binary=false): bytes to base64, dates parsed",
			preserveBinary: false,
			input: map[string]any{
				"name": "test_doc",
				"data": rawBytes,
				"created_at": map[string]any{
					"$date": map[string]any{
						"$numberLong": "1793011200000",
					},
				},
			},
			validate: func(t *testing.T, result any) {
				doc, ok := result.(bson.D)
				require.True(t, ok)

				require.Len(t, doc, 3)
				assert.Equal(t, "created_at", doc[0].Key)
				assert.Equal(t, "data", doc[1].Key)
				assert.Equal(t, "name", doc[2].Key)

				assert.Equal(t, bson.DateTime(int64(1793011200000)), doc[0].Value)
				assert.Equal(t, expectedBase64, doc[1].Value)
			},
		},
		{
			name:           "Preserve binary mode (preserve_binary=true): bytes kept raw, dates parsed",
			preserveBinary: true,
			input: map[string]any{
				"name": "test_doc",
				"data": rawBytes,
				"created_at": map[string]any{
					"$date": map[string]any{
						"$numberLong": "1793011200001",
					},
				},
			},
			validate: func(t *testing.T, result any) {
				doc, ok := result.(bson.D)
				require.True(t, ok)

				require.Len(t, doc, 3)
				assert.Equal(t, "created_at", doc[0].Key)
				assert.Equal(t, "data", doc[1].Key)
				assert.Equal(t, "name", doc[2].Key)

				assert.Equal(t, bson.DateTime(int64(1793011200001)), doc[0].Value)
				assert.Equal(t, rawBytes, doc[1].Value)
			},
		},
		{
			name:           "Deep nested sorting and type evaluation",
			preserveBinary: true,
			input: map[string]any{
				"root": map[string]any{
					"z_field": "im_last",
					"a_field": map[string]any{
						"$oid": "60c72b2f9b1d8b2bad000001",
					},
					"nested_bytes": rawBytes,
				},
			},
			validate: func(t *testing.T, result any) {
				doc, ok := result.(bson.D)
				require.True(t, ok)

				rootVal := doc[0].Value
				rootDoc, ok := rootVal.(bson.D)
				require.True(t, ok)

				require.Len(t, rootDoc, 3)
				assert.Equal(t, "a_field", rootDoc[0].Key)
				assert.Equal(t, "nested_bytes", rootDoc[1].Key)
				assert.Equal(t, "z_field", rootDoc[2].Key)

				expectedOID, err := bson.ObjectIDFromHex("60c72b2f9b1d8b2bad000001")
				assert.NoError(t, err)
				assert.Equal(t, expectedOID, rootDoc[0].Value)
				assert.Equal(t, rawBytes, rootDoc[1].Value)
			},
		},
		{
			name:           "Arrays containing maps are recursively processed",
			preserveBinary: true,
			input: []any{
				map[string]any{
					"b": rawBytes,
					"a": map[string]any{"$numberInt": "42"},
				},
			},
			validate: func(t *testing.T, result any) {
				slice, ok := result.([]any)
				require.True(t, ok)
				require.Len(t, slice, 1)

				itemDoc, ok := slice[0].(bson.D)
				require.True(t, ok)

				assert.Equal(t, "a", itemDoc[0].Key)
				assert.Equal(t, int32(42), itemDoc[0].Value)
				assert.Equal(t, "b", itemDoc[1].Key)
				assert.Equal(t, rawBytes, itemDoc[1].Value)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := processExtJSON(tt.input, tt.preserveBinary)
			assert.NoError(t, err)
			tt.validate(t, res)
		})
	}
}

func TestConvertExtJSONObject(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{
			name: "Nested extJSON element",
			input: map[string]any{
				"name": "test_doc",
				"data": []byte("hello"),
				"created_at": map[string]any{
					"$date": map[string]any{
						"$numberLong": "1793011200000",
					},
				},
			},
			expected: bson.D{
				bson.E{Key: "created_at", Value: bson.DateTime(int64(1793011200000))},
				bson.E{Key: "data", Value: "aGVsbG8="},
				bson.E{Key: "name", Value: "test_doc"},
			},
		},
		{
			name: "Top level extJSON element",
			input: map[string]any{
				"$date": map[string]any{
					"$numberLong": "1793011200000",
				},
			},
			expected: bson.DateTime(int64(1793011200000)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := convertExtJSONObject(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, res)
		})
	}
}
