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

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	franz_sr "github.com/twmb/franz-go/pkg/sr"
)

func runRedpandaPairForSchemaMigration(t *testing.T) (src, dst redpandatest.Endpoints) {
	var err error
	src, err = redpandatest.StartRedpanda(t, true)
	require.NoError(t, err)
	dst, err = redpandatest.StartRedpanda(t, true)
	require.NoError(t, err)
	return
}

// setGlobalImportMode switches the schema registry at the given URL into IMPORT mode. The `schema_registry` output
// requires this on the destination whenever `translate_ids` is `false` (the default), since schemas are then created
// with their original IDs and versions.
func setGlobalImportMode(t *testing.T, url string) {
	t.Helper()

	client, err := franz_sr.NewClient(franz_sr.URLs(url))
	require.NoError(t, err)

	res := client.SetMode(t.Context(), franz_sr.ModeImport)
	require.Len(t, res, 1)
	require.NoError(t, res[0].Err)
	require.Equal(t, franz_sr.ModeImport, res[0].Mode)
}

// requireGlobalMode asserts the global mode of the schema registry at the given URL. Besides checking a precondition,
// this also warms up a freshly started registry, whose first request can take several seconds while it creates its
// backing topic.
func requireGlobalMode(t *testing.T, url string, want franz_sr.Mode) {
	t.Helper()

	client, err := franz_sr.NewClient(franz_sr.URLs(url))
	require.NoError(t, err)

	res := client.Mode(t.Context())
	require.Len(t, res, 1)
	require.NoError(t, res[0].Err)
	require.Equal(t, want, res[0].Mode)
}

// createSchemaWithID registers a schema under the given subject with a fixed ID and version. The registry must be in
// IMPORT mode.
func createSchemaWithID(t *testing.T, url, subject, schema string, id, version int) {
	t.Helper()

	client, err := franz_sr.NewClient(franz_sr.URLs(url))
	require.NoError(t, err)

	_, err = client.CreateSchemaWithIDAndVersion(t.Context(), subject, franz_sr.Schema{Schema: schema}, id, version)
	require.NoError(t, err)
}

// schemasEqual compares two schema objects for equality, ignoring newlines and leading/trailing spaces in the schema
// string, since registries may reformat normalised schemas.
func schemasEqual(lhs, rhs franz_sr.Schema) bool {
	lhsSchema := strings.TrimSpace(strings.ReplaceAll(lhs.Schema, "\n", ""))
	rhsSchema := strings.TrimSpace(strings.ReplaceAll(rhs.Schema, "\n", ""))
	if lhsSchema != rhsSchema {
		return false
	}
	return cmp.Equal(lhs, rhs, cmpopts.IgnoreFields(franz_sr.Schema{}, "Schema"))
}

// logCapture is a service.PrintLogger which records every line logged by a stream.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (l *logCapture) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

func (l *logCapture) Println(v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintln(v...))
}

func (l *logCapture) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

func (l *logCapture) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "")
}

// runSchemaMigrationStream runs a `schema_registry` input -> `schema_registry` output stream from src to dst with
// `translate_ids: false`, logging and dropping failed writes instead of retrying them, and returns the captured logs.
func runSchemaMigrationStream(t *testing.T, src, dst redpandatest.Endpoints) *logCapture {
	t.Helper()

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
output:
  fallback:
    - schema_registry:
        url: %s
        subject: ${! @schema_registry_subject }
        translate_ids: false
    - drop: {}
      processors:
        - log:
            level: ERROR
            message: 'schema write failed: ${! @fallback_error }'
`, src.SchemaRegistryURL, dst.SchemaRegistryURL)))

	logs := &logCapture{}
	streamBuilder.SetPrintLogger(logs)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 5*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx), "logs:\n%s", logs.String())

	return logs
}

// requireSubjectVersionStatus asserts the HTTP status returned when fetching a subject version from a registry.
func requireSubjectVersionStatus(t *testing.T, url, subject string, version, wantStatus int) {
	t.Helper()

	resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/%d", url, subject, version))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, wantStatus, resp.StatusCode)
}

func TestSchemaRegistryIntegration(t *testing.T) {
	integration.CheckSkip(t)

	dummySchema := `{"name":"foo", "type": "string"}`
	dummySchemaWithReference := `{"name":"bar", "type": "record", "fields":[{"name":"data", "type": "foo"}]}`
	tests := []struct {
		name                       string
		includeSoftDeletedSubjects bool
		extraSubject               string
		subjectFilter              string
		schemaWithReference        bool
	}{
		{
			name: "roundtrip",
		},
		{
			name:                       "roundtrip with deleted subject",
			includeSoftDeletedSubjects: true,
		},
		{
			name:          "roundtrip with subject filter",
			extraSubject:  "foobar",
			subjectFilter: `^\w+-\w+-\w+-\w+-\w+$`,
		},
		{
			name: "roundtrip with schema references",
			// A UUID which always gets picked first when querying the `/subjects` endpoint.
			extraSubject:        "ffffffff-ffff-ffff-ffff-ffffffffffff",
			schemaWithReference: true,
		},
	}

	src, dst := runRedpandaPairForSchemaMigration(t)
	setGlobalImportMode(t, dst.SchemaRegistryURL)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			u4, err := uuid.NewV4()
			require.NoError(t, err)
			subject := u4.String()

			defer func() {
				// Clean up the extraSubject first since it may contain schemas with references.
				if test.extraSubject != "" {
					deleteSubject(t, src.SchemaRegistryURL, test.extraSubject, false)
					deleteSubject(t, src.SchemaRegistryURL, test.extraSubject, true)
					if test.subjectFilter == "" {
						deleteSubject(t, dst.SchemaRegistryURL, test.extraSubject, false)
						deleteSubject(t, dst.SchemaRegistryURL, test.extraSubject, true)
					}
				}

				if !test.includeSoftDeletedSubjects {
					deleteSubject(t, src.SchemaRegistryURL, subject, false)
				}
				deleteSubject(t, src.SchemaRegistryURL, subject, true)

				deleteSubject(t, dst.SchemaRegistryURL, subject, false)
				deleteSubject(t, dst.SchemaRegistryURL, subject, true)
			}()

			createSchema(t, src.SchemaRegistryURL, subject, dummySchema, nil)

			if test.subjectFilter != "" {
				createSchema(t, src.SchemaRegistryURL, test.extraSubject, dummySchema, nil)
			}

			if test.includeSoftDeletedSubjects {
				deleteSubject(t, src.SchemaRegistryURL, subject, false)
			}

			if test.schemaWithReference {
				createSchema(t, src.SchemaRegistryURL, test.extraSubject, dummySchemaWithReference, []franz_sr.SchemaReference{{Name: "foo", Subject: subject, Version: 1}})
			}

			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
    include_deleted: %t
    subject_filter: %s
    fetch_in_order: %t
output:
  fallback:
    - schema_registry:
        url: %s
        subject: ${! @schema_registry_subject }
        # Preserve schema order.
        max_in_flight: 1
    # Don't retry the same message multiple times so we do fail if schemas with references are sent in the wrong order
    - drop: {}
`, src.SchemaRegistryURL, test.includeSoftDeletedSubjects, test.subjectFilter, test.schemaWithReference, dst.SchemaRegistryURL)))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

			stream, err := streamBuilder.Build()
			require.NoError(t, err)

			ctx, done := context.WithTimeout(t.Context(), 3*time.Second)
			defer done()

			err = stream.Run(ctx)
			require.NoError(t, err)

			defer func() {
				require.NoError(t, stream.StopWithin(1*time.Second))
			}()

			resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects", dst.SchemaRegistryURL))
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			if test.subjectFilter != "" {
				assert.Contains(t, string(body), subject)
				assert.NotContains(t, string(body), test.extraSubject)
			}

			resp, err = http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/1", dst.SchemaRegistryURL, subject))
			require.NoError(t, err)
			body, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var sd franz_sr.SubjectSchema
			require.NoError(t, json.Unmarshal(body, &sd))
			assert.Equal(t, subject, sd.Subject)
			assert.Equal(t, 1, sd.Version)
			assert.JSONEq(t, dummySchema, sd.Schema.Schema)

			if test.schemaWithReference {
				resp, err = http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/1", dst.SchemaRegistryURL, test.extraSubject))
				require.NoError(t, err)
				body, err = io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				require.Equal(t, http.StatusOK, resp.StatusCode)

				var sd franz_sr.SubjectSchema
				require.NoError(t, json.Unmarshal(body, &sd))
				assert.Equal(t, test.extraSubject, sd.Subject)
				assert.Equal(t, 1, sd.Version)
				assert.JSONEq(t, dummySchemaWithReference, sd.Schema.Schema)
			}
		})
	}
}

func writeSchema(t *testing.T, sr redpandatest.Endpoints, schema []byte, normalize, removeMetadata, removeRuleSet bool) {
	streamBuilder := service.NewStreamBuilder()

	// Set up a dummy `schema_registry` input which the output can connect to even though it won't need to fetch any
	// schemas from it.
	input := fmt.Sprintf(`
schema_registry:
  url: %s
  subject_filter: does_not_exist
`, sr.SchemaRegistryURL)
	require.NoError(t, streamBuilder.AddInputYAML(input))

	output := fmt.Sprintf(`
schema_registry:
  url: %s
  subject: ${! json("subject") }
  backfill_dependencies: true
  normalize: %t
  remove_metadata: %t
  remove_rule_set: %t
`, sr.SchemaRegistryURL, normalize, removeMetadata, removeRuleSet)
	require.NoError(t, streamBuilder.AddOutputYAML(output))

	prodFn, err := streamBuilder.AddProducerFunc()
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	doneChan := make(chan struct{})
	go func() {
		require.NoError(t, stream.Run(t.Context()))
		close(doneChan)
	}()
	defer func() {
		require.NoError(t, stream.StopWithin(3*time.Second))
		<-doneChan
	}()

	require.NoError(t, prodFn(t.Context(), service.NewMessage(schema)))
}

func TestSchemaRegistryProtobufSchemasIntegration(t *testing.T) {
	integration.CheckSkip(t)

	sr, err := redpandatest.StartRedpanda(t, true)
	require.NoError(t, err)
	setGlobalImportMode(t, sr.SchemaRegistryURL)

	t.Logf("Schema Registry URL: %s", sr.SchemaRegistryURL)

	testFn := func(t *testing.T, subject string, normalize bool, metadata, ruleSet string) {
		const dummyProtoSchema = `syntax = "proto3";
package com.mycorp.mynamespace;

message SampleRecord {
  int32 my_field1 = 1;
  double my_field2 = 2;
  string my_field3 = 3;
}`

		// This denormalized schema has 2 fields in a different order than the normalized one.
		const dummyDenormalizedProtoSchema = `syntax = "proto3";
package com.mycorp.mynamespace;

message SampleRecord {
  int32 my_field1 = 1;
  string my_field3 = 3;
  double my_field2 = 2;
}`

		dummySchema := dummyProtoSchema
		if normalize {
			dummySchema = dummyDenormalizedProtoSchema
		}

		var schemaMetadata *franz_sr.SchemaMetadata
		if metadata != "" {
			require.NoError(t, json.Unmarshal([]byte(metadata), &schemaMetadata))
		}
		var schemaRuleSet *franz_sr.SchemaRuleSet
		if ruleSet != "" {
			require.NoError(t, json.Unmarshal([]byte(ruleSet), &schemaRuleSet))
		}

		inputSS := franz_sr.SubjectSchema{
			Subject: subject,
			Version: 1,
			ID:      1,
			Schema: franz_sr.Schema{
				Schema:         dummySchema,
				Type:           franz_sr.TypeProtobuf,
				SchemaMetadata: schemaMetadata,
				SchemaRuleSet:  schemaRuleSet,
			},
		}
		schema, err := json.Marshal(inputSS)
		require.NoError(t, err)

		writeSchema(t, sr, schema, normalize, metadata != "", ruleSet != "")

		resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/%d", sr.SchemaRegistryURL, subject, 1))
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var returnedSS franz_sr.SubjectSchema
		require.NoError(t, json.Unmarshal(body, &returnedSS))
		assert.Equal(t, subject, returnedSS.Subject)
		assert.Equal(t, 1, returnedSS.Version)

		if normalize {
			inputSS.Schema.Schema = dummyProtoSchema
		}
		if metadata != "" {
			inputSS.SchemaMetadata = nil
		}
		if ruleSet != "" {
			inputSS.SchemaRuleSet = nil
		}
		assert.True(t, schemasEqual(inputSS.Schema, returnedSS.Schema))
	}

	const dummySubject = "foo"

	deleteDummySubject := func() {
		// Clean up the subject at the end of each subtest.
		deleteSubject(t, sr.SchemaRegistryURL, dummySubject, false)
		deleteSubject(t, sr.SchemaRegistryURL, dummySubject, true)
	}

	t.Run("allows creating the same schema twice", func(t *testing.T) {
		defer deleteDummySubject()

		for range 2 {
			testFn(t, dummySubject, false, "", "")
		}
	})

	t.Run("normalises schemas", func(t *testing.T) {
		defer deleteDummySubject()

		testFn(t, dummySubject, true, "", "")
	})

	t.Run("removes metadata", func(t *testing.T) {
		defer deleteDummySubject()

		const metadata = `{
  "properties": {
    "confluent:version": "1"
  }
}`
		testFn(t, dummySubject, true, metadata, "")
	})

	t.Run("removes rule sets", func(t *testing.T) {
		defer deleteDummySubject()

		const ruleSet = `{
  "domainRules": [
    {
      "name": "checkSsnLen",
      "kind": "CONDITION",
      "type": "CEL",
      "mode": "WRITE",
      "expr": "size(message.ssn) == 9"
    }
  ]
}`
		testFn(t, dummySubject, true, "", ruleSet)
	})

	t.Run("associates the same schema with multiple subjects", func(t *testing.T) {
		extraSubject := "bar"

		testFn(t, dummySubject, false, "", "")
		testFn(t, extraSubject, false, "", "")

		// Cleanup the extra subject.
		deleteSubject(t, sr.SchemaRegistryURL, extraSubject, false)
		deleteSubject(t, sr.SchemaRegistryURL, extraSubject, true)
	})
}

func TestSchemaRegistryDuplicateSchemaIntegration(t *testing.T) {
	integration.CheckSkip(t)

	src, dst := runRedpandaPairForSchemaMigration(t)
	setGlobalImportMode(t, dst.SchemaRegistryURL)

	dummySubject := "foobar"
	dummySchema := `{"name":"foo", "type": "string"}`
	createSchema(t, src.SchemaRegistryURL, dummySubject, dummySchema, nil)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
output:
  schema_registry:
    url: %s
    subject: ${! @schema_registry_subject }
    translate_ids: false
`, src.SchemaRegistryURL, dst.SchemaRegistryURL)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	runStream := func() {
		stream, err := streamBuilder.Build()
		require.NoError(t, err)

		ctx, done := context.WithTimeout(t.Context(), 2*time.Second)
		defer done()
		err = stream.Run(ctx)
		require.NoError(t, err)
	}

	runStream()
	// The second run should perform an idempotent write for the same schema and not fail.
	runStream()

	dummyVersion := 1
	resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/%d", dst.SchemaRegistryURL, dummySubject, dummyVersion))
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var sd franz_sr.SubjectSchema
	require.NoError(t, json.Unmarshal(body, &sd))
	assert.Equal(t, dummySubject, sd.Subject)
	assert.Equal(t, 1, sd.Version)
	assert.JSONEq(t, dummySchema, sd.Schema.Schema)
}

func TestSchemaRegistryConflictingSchemaIntegration(t *testing.T) {
	integration.CheckSkip(t)

	src, dst := runRedpandaPairForSchemaMigration(t)
	setGlobalImportMode(t, dst.SchemaRegistryURL)

	dummySubject := "foobar"
	createSchema(t, src.SchemaRegistryURL, dummySubject, `{"name":"foo", "type": "string"}`, nil)

	// Register a different schema under the same ID at the destination so that the migration is a genuine conflict.
	createSchemaWithID(t, dst.SchemaRegistryURL, "other", `{"name":"other", "type": "int"}`, 1, 1)

	logs := runSchemaMigrationStream(t, src, dst)

	// The conflict must be surfaced as an error rather than swallowed, and the subject must not be created.
	assert.True(t, logs.contains("Overwrite new schema with id 1 is not permitted"), "logs:\n%s", logs.String())
	requireSubjectVersionStatus(t, dst.SchemaRegistryURL, dummySubject, 1, http.StatusNotFound)
}

func TestSchemaRegistryReadWriteDestinationIntegration(t *testing.T) {
	integration.CheckSkip(t)

	// The destination is deliberately left in READWRITE mode, which rejects schemas created with their original IDs.
	src, dst := runRedpandaPairForSchemaMigration(t)
	requireGlobalMode(t, dst.SchemaRegistryURL, franz_sr.ModeReadWrite)

	dummySubject := "foobar"
	createSchema(t, src.SchemaRegistryURL, dummySubject, `{"name":"foo", "type": "string"}`, nil)

	logs := runSchemaMigrationStream(t, src, dst)

	assert.True(t, logs.contains("translate_ids is false"), "expected a mode mismatch warning at connect time, logs:\n%s", logs.String())
	assert.True(t, logs.contains("is not in import mode"), "logs:\n%s", logs.String())
	requireSubjectVersionStatus(t, dst.SchemaRegistryURL, dummySubject, 1, http.StatusNotFound)
}

func TestSchemaRegistryIDTranslationIntegration(t *testing.T) {
	integration.CheckSkip(t)

	src, dst := runRedpandaPairForSchemaMigration(t)

	// Create two schemas under subject `foo`.
	createSchema(t, src.SchemaRegistryURL, "foo", `{"name":"foo", "type": "record", "fields":[{"name":"str", "type": "string"}]}`, nil)
	createSchema(t, src.SchemaRegistryURL, "foo", `{"name":"foo", "type": "record", "fields":[{"name":"str", "type": "string"}, {"name":"num", "type": "int", "default": 42}]}`, nil)

	// Create a schema under subject `bar` which references the second schema under `foo`.
	createSchema(t, src.SchemaRegistryURL, "bar", `{"name":"bar", "type": "record", "fields":[{"name":"data", "type": "foo"}]}`,
		[]franz_sr.SchemaReference{{Name: "foo", Subject: "foo", Version: 2}},
	)

	// Create a schema at the dst which will have ID 1 so we can check that the ID translation works
	// correctly.
	createSchema(t, dst.SchemaRegistryURL, "baz", `{"name":"baz", "type": "record", "fields":[{"name":"num", "type": "int"}]}`, nil)

	// Use a Stream with a mapping filter to send only the schema with the reference to the dst in order
	// to force the output to backfill the rest of the schemas.
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
  processors:
    - mapping: |
        if this.id != 3 { root = deleted() }
output:
  fallback:
    - schema_registry:
        url: %s
        subject: ${! @schema_registry_subject }
        # Preserve schema order
        max_in_flight: 1
        translate_ids: true
    # Don't retry the same message multiple times so we do fail if schemas with references are sent in the wrong order
    - drop: {}
`, src.SchemaRegistryURL, dst.SchemaRegistryURL)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 3*time.Second)
	defer done()

	err = stream.Run(ctx)
	require.NoError(t, err)

	// Check that the schemas were backfilled correctly.
	tests := []struct {
		subject            string
		version            int
		expectedID         int
		expectedReferences []franz_sr.SchemaReference
	}{
		{
			subject:    "foo",
			version:    1,
			expectedID: 2,
		},
		{
			subject:    "foo",
			version:    2,
			expectedID: 3,
		},
		{
			subject:            "bar",
			version:            1,
			expectedID:         4,
			expectedReferences: []franz_sr.SchemaReference{{Name: "foo", Subject: "foo", Version: 2}},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/%d", dst.SchemaRegistryURL, test.subject, test.version))
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var sd franz_sr.SubjectSchema
			require.NoError(t, json.Unmarshal(body, &sd))
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, test.expectedID, sd.ID)
			assert.Equal(t, test.expectedReferences, sd.References)
		})
	}
}

func TestSchemaRegistryCompatibilityLevelIntegration(t *testing.T) {
	integration.CheckSkip(t)

	src, dst := runRedpandaPairForSchemaMigration(t)
	setGlobalImportMode(t, dst.SchemaRegistryURL)

	compatLevel := franz_sr.CompatFull

	// Generate a unique subject name
	u4, err := uuid.NewV4()
	require.NoError(t, err)
	subject := fmt.Sprintf("compatibility-test-%s", u4.String())

	// Define a simple schema
	schema := `{"type":"record","name":"test","fields":[{"name":"field1","type":"string"}]}`

	// Create schema in source registry
	createSchema(t, src.SchemaRegistryURL, subject, schema, nil)

	// Set compatibility level on the source subject first
	srcClient, err := franz_sr.NewClient(franz_sr.URLs(src.SchemaRegistryURL))
	require.NoError(t, err)
	setCompatResp := srcClient.SetCompatibility(t.Context(), franz_sr.SetCompatibility{
		Level: compatLevel,
	}, subject)
	require.NoError(t, setCompatResp[0].Err)

	// Verify the compatibility level was set correctly on source
	compatRespSrc := srcClient.Compatibility(t.Context(), subject)
	require.NoError(t, compatRespSrc[0].Err)
	assert.Equal(t, compatLevel, compatRespSrc[0].Level, "Source compatibility level not set correctly")

	// Create a stream that transfers the schema and compatibility level
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
    subject_filter: %s
output:
  schema_registry:
    url: %s
    subject: ${! @schema_registry_subject }
    subject_compatibility_level: ${! @schema_registry_subject_compatibility_level }
    max_in_flight: 1
`, src.SchemaRegistryURL, subject, dst.SchemaRegistryURL)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	// Run the stream with a timeout
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	require.NoError(t, stream.Run(ctx))
	require.NoError(t, stream.StopWithin(1*time.Second))

	// Verify the compatibility level was propagated to the destination
	dstClient, err := franz_sr.NewClient(franz_sr.URLs(dst.SchemaRegistryURL))
	require.NoError(t, err)
	compatRespDst := dstClient.Compatibility(t.Context(), subject)
	require.NoError(t, compatRespDst[0].Err)
	assert.Equal(t, compatLevel, compatRespDst[0].Level,
		"Compatibility level not properly propagated to destination")
}

func TestSchemaRegistryMaxInFlightIntegration(t *testing.T) {
	integration.CheckSkip(t)

	src, dst := runRedpandaPairForSchemaMigration(t)
	setGlobalImportMode(t, dst.SchemaRegistryURL)

	u4, err := uuid.NewV4()
	require.NoError(t, err)
	baseSubject := u4.String()

	// Create 10 schemas, each referencing the previous one
	// First schema is a basic type
	firstSchema := `{"name":"schema_0", "type": "string"}`
	firstSubject := fmt.Sprintf("%s-%d", baseSubject, 0)
	createSchema(t, src.SchemaRegistryURL, firstSubject, firstSchema, nil)

	// Create 9 more schemas with references to the previous ones
	for i := 1; i < 100; i++ {
		prevSubject := fmt.Sprintf("%s-%d", baseSubject, i-1)
		subject := fmt.Sprintf("%s-%d", baseSubject, i)

		schema := fmt.Sprintf(`{
			"name": "schema_%d",
			"type": "record",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "reference_data", "type": "schema_%d"}
			]
		}`, i, i-1)

		references := []franz_sr.SchemaReference{
			{
				Name:    fmt.Sprintf("schema_%d", i-1),
				Subject: prevSubject,
				Version: 1,
			},
		}

		t.Logf("Creating schema %s with references to %s", subject, prevSubject)
		createSchema(t, src.SchemaRegistryURL, subject, schema, references)
	}

	// Create a stream with max_in_flight: 2 to test dependent schema migration
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
output:
  fallback:
    - schema_registry:
        url: %s
        subject: ${! @schema_registry_subject }
        # Limited concurrency to test ordering with dependencies
        max_in_flight: 5
    - drop: {}
logger:
  level: TRACE
`, src.SchemaRegistryURL, dst.SchemaRegistryURL)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: DEBUG`))

	require.NoError(t, streamBuilder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
		time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 10*time.Second)
	defer done()

	require.NoError(t, stream.Run(ctx))

	// Verify all schemas migrated correctly
	for i := range 100 {
		subject := fmt.Sprintf("%s-%d", baseSubject, i)

		resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/1", dst.SchemaRegistryURL, subject))
		require.NoError(t, err)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "Failed to get schema for subject %s", subject)

		var sd franz_sr.SubjectSchema
		require.NoError(t, json.Unmarshal(body, &sd))

		assert.Equal(t, subject, sd.Subject)
		assert.Equal(t, 1, sd.Version)

		// For non-first schema, check that reference exists
		if i > 0 {
			assert.NotEmpty(t, sd.References)
			foundRef := false
			for _, ref := range sd.References {
				if ref.Subject == fmt.Sprintf("%s-%d", baseSubject, i-1) {
					foundRef = true
					break
				}
			}
			assert.True(t, foundRef, "Schema %d should reference schema %d", i, i-1)
		}
	}
}

func createSchema(t *testing.T, url, subject, schema string, references []franz_sr.SchemaReference) {
	t.Helper()

	client, err := franz_sr.NewClient(franz_sr.URLs(url))
	require.NoError(t, err)

	_, err = client.CreateSchema(t.Context(), subject, franz_sr.Schema{Schema: schema, References: references})
	require.NoError(t, err)
}

func deleteSubject(t *testing.T, url, subject string, hardDelete bool) {
	t.Helper()

	client, err := franz_sr.NewClient(franz_sr.URLs(url))
	require.NoError(t, err)

	deleteMode := franz_sr.SoftDelete
	if hardDelete {
		deleteMode = franz_sr.HardDelete
	}

	_, err = client.DeleteSubject(t.Context(), subject, deleteMode)
	require.NoError(t, err)
}
