// Copyright 2025 Redpanda Data, Inc.
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

package migrator_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/migrator"
)

func startSchemaRegistrySourceAndDestination(t *testing.T, opts ...redpandatestConfigOpt) (*sr.Client, *sr.Client) {
	src, dst := startRedpandaSourceAndDestination(t, opts...)
	srSrc, err := sr.NewClient(sr.URLs(src.SchemaRegistryURL))
	require.NoError(t, err)
	srDst, err := sr.NewClient(sr.URLs(dst.SchemaRegistryURL))
	require.NoError(t, err)

	return srSrc, srDst
}

// Use compatible Avro record evolution for multi: add fields with defaults
const (
	dummyAvroSchemaV1 = `{
        "type": "record",
        "name": "MultiRecord",
        "fields": [
            {"name": "a", "type": "int"}
        ]
    }`

	dummyAvroSchemaV2 = `{
        "type": "record",
        "name": "MultiRecord",
        "fields": [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "int", "default": 0}
        ]
    }`

	dummyAvroSchemaV3 = `{
        "type": "record",
        "name": "MultiRecord",
        "fields": [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "int", "default": 0},
            {"name": "c", "type": "int", "default": 0}
        ]
    }`
)

func TestIntegrationSchemaRegistryMigratorListSubjectSchemas(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: Schema Registry")
	src, dst := startSchemaRegistrySourceAndDestination(t)

	const (
		subjFoo1  = "foo-1"
		subjFoo2  = "foo-2"
		subjDel   = "deleted"
		subjMulti = "multi"
	)

	createSchema := func(subject, schema string) int {
		t.Helper()
		ss, err := src.CreateSchema(t.Context(), subject, sr.Schema{Schema: schema})
		require.NoError(t, err)
		return ss.Version
	}
	softDeleteSubject := func(subject string) {
		t.Helper()
		_, err := src.DeleteSubject(t.Context(), subject, sr.SoftDelete)
		require.NoError(t, err)
	}
	softDeleteSchemaVersion := func(subject string, version int) {
		t.Helper()
		err := src.DeleteSchema(t.Context(), subject, version, sr.SoftDelete)
		require.NoError(t, err)
	}

	const dummy = `{"type":"string"}`
	createSchema(subjFoo1, dummy)
	createSchema(subjFoo2, dummy)
	createSchema(subjDel, dummy)
	softDeleteSubject(subjDel)

	createSchema(subjMulti, dummyAvroSchemaV1)
	createSchema(subjMulti, dummyAvroSchemaV2)
	v3ID := createSchema(subjMulti, dummyAvroSchemaV3)
	softDeleteSchemaVersion(subjMulti, v3ID)

	// Thin schema representation for comparisons: only Version and Schema
	type sv struct {
		Subject string
		Version int
	}

	list := func(t *testing.T, conf migrator.SchemaRegistryMigratorConfig) []sv {
		m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)
		ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
		defer cancel()
		ss, err := m.ListSubjectSchemas(ctx)
		require.NoError(t, err)

		res := make([]sv, 0, len(ss))
		for _, v := range ss {
			res = append(res, sv{Subject: v.Subject, Version: v.Version})
		}
		return res
	}

	t.Run("latest", func(t *testing.T) {
		t.Parallel()

		got := list(t, migrator.SchemaRegistryMigratorConfig{Versions: migrator.VersionsLatest})
		exp := []sv{
			{Subject: subjFoo1, Version: 1},
			{Subject: subjFoo2, Version: 1},
			{Subject: subjMulti, Version: 2},
		}
		assert.ElementsMatch(t, exp, got)
	})

	t.Run("latest include", func(t *testing.T) {
		t.Parallel()

		conf := migrator.SchemaRegistryMigratorConfig{Versions: migrator.VersionsLatest}
		conf.Include = []*regexp.Regexp{regexp.MustCompile(`^foo-.*$`)}
		got := list(t, conf)
		exp := []sv{
			{Subject: subjFoo1, Version: 1},
			{Subject: subjFoo2, Version: 1},
		}
		assert.ElementsMatch(t, exp, got)
	})

	t.Run("latest include exclude", func(t *testing.T) {
		t.Parallel()

		conf := migrator.SchemaRegistryMigratorConfig{Versions: migrator.VersionsLatest}
		conf.Include = []*regexp.Regexp{regexp.MustCompile(`^foo-.*$`)}
		conf.Exclude = []*regexp.Regexp{regexp.MustCompile(`^foo-2$`)}
		got := list(t, conf)
		exp := []sv{
			{Subject: subjFoo1, Version: 1},
		}
		assert.ElementsMatch(t, exp, got)
	})

	t.Run("latest deleted", func(t *testing.T) {
		t.Parallel()

		conf := migrator.SchemaRegistryMigratorConfig{
			Versions:       migrator.VersionsLatest,
			IncludeDeleted: true,
		}
		got := list(t, conf)
		exp := []sv{
			{Subject: subjFoo1, Version: 1},
			{Subject: subjFoo2, Version: 1},
			{Subject: subjMulti, Version: 3},
			{Subject: subjDel, Version: 1},
		}
		assert.ElementsMatch(t, exp, got)
	})

	t.Run("all versions", func(t *testing.T) {
		t.Parallel()

		conf := migrator.SchemaRegistryMigratorConfig{
			Versions: migrator.VersionsAll,
		}
		got := list(t, conf)
		exp := []sv{
			{Subject: subjFoo1, Version: 1},
			{Subject: subjFoo2, Version: 1},
			{Subject: subjMulti, Version: 1},
			{Subject: subjMulti, Version: 2},
		}
		assert.ElementsMatch(t, exp, got)
	})

	t.Run("all versions including deleted", func(t *testing.T) {
		t.Parallel()

		conf := migrator.SchemaRegistryMigratorConfig{
			Versions:       migrator.VersionsAll,
			IncludeDeleted: true,
		}
		got := list(t, conf)
		exp := []sv{
			{Subject: subjFoo1, Version: 1},
			{Subject: subjFoo2, Version: 1},
			{Subject: subjDel, Version: 1},
			{Subject: subjMulti, Version: 1},
			{Subject: subjMulti, Version: 2},
			{Subject: subjMulti, Version: 3},
		}
		assert.ElementsMatch(t, exp, got)
	})
}

func TestIntegrationSchemaRegistryMigratorSyncNameResolver(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: source and destination Schema Registry")
	src, dst := startSchemaRegistrySourceAndDestination(t)

	t.Log("When: a source contains a schema")
	const (
		subj   = "foo"
		schema = `{"type":"string"}`
	)
	_, err := src.CreateSchema(t.Context(), subj, sr.Schema{Schema: schema})
	require.NoError(t, err)

	t.Log("And: destination is set to import mode")
	modeRes := dst.SetMode(t.Context(), sr.ModeImport)
	require.NoError(t, modeRes[0].Err)

	nr, err := service.NewInterpolatedString("dst_${! @schema_registry_subject }")
	require.NoError(t, err)

	t.Log("And: migrator is configured with name resolver")
	conf := migrator.SchemaRegistryMigratorConfig{
		Enabled:      true,
		Versions:     migrator.VersionsLatest,
		NameResolver: nr,
	}
	m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)

	t.Log("When: migrator is run")
	ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
	defer cancel()
	require.NoError(t, m.Sync(ctx))

	t.Log("Then: destination contains renamed subject")
	sd, err := dst.SchemaByVersion(ctx, "dst_"+subj, 1)
	require.NoError(t, err)
	assert.Equal(t, "dst_"+subj, sd.Subject)
	assert.Equal(t, 1, sd.Version)
}

func TestIntegrationSchemaRegistryMigratorSyncVersionsAll(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: source and destination Schema Registry")
	src, dst := startSchemaRegistrySourceAndDestination(t)

	t.Log("When: two schema versions exist at source")
	const subj = "multi"

	_, err := src.CreateSchema(t.Context(), subj, sr.Schema{Schema: dummyAvroSchemaV1})
	require.NoError(t, err)
	_, err = src.CreateSchema(t.Context(), subj, sr.Schema{Schema: dummyAvroSchemaV2})
	require.NoError(t, err)

	t.Log("And: destination is set to import mode")
	modeRes := dst.SetMode(t.Context(), sr.ModeImport)
	require.NoError(t, modeRes[0].Err)

	t.Log("And: migrator is configured with all versions")
	conf := migrator.SchemaRegistryMigratorConfig{
		Enabled:  true,
		Versions: migrator.VersionsAll,
	}
	m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)

	t.Log("When: migrator is run")
	ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
	defer cancel()
	require.NoError(t, m.Sync(ctx))

	t.Log("Then: both versions exist at destination")
	sd1, err := dst.SchemaByVersion(ctx, subj, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, sd1.Version)
	sd1s := sd1.Schema.Schema
	assert.True(t, migrator.SchemaStringEquals(dummyAvroSchemaV1, sd1s, sd1.Type))

	sd2, err := dst.SchemaByVersion(ctx, subj, 2)
	require.NoError(t, err)
	assert.Equal(t, 2, sd2.Version)
	sd2s := sd2.Schema.Schema
	assert.True(t, migrator.SchemaStringEquals(dummyAvroSchemaV2, sd2s, sd2.Type))
}

func TestIntegrationSchemaRegistryMigratorSyncWithReferences(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: source and destination Schema Registry")
	src, dst := startSchemaRegistrySourceAndDestination(t)

	ctx := t.Context()

	t.Log("When: address schema is created as reference schema with fixed ID")
	const (
		addressSubject = "address01-value"
		addressSchema  = `{"type":"record","name":"Address","namespace":"com.example.schemas","fields":[{"name":"street","type":"string"},{"name":"city","type":"string"},{"name":"state","type":"string"},{"name":"zipCode","type":"string"}]}`
	)

	t.Log("And: source and destination address subject is set to import mode")
	modeRes := src.SetMode(ctx, sr.ModeImport, addressSubject)
	require.NoError(t, modeRes[0].Err)
	modeRes = dst.SetMode(ctx, sr.ModeImport, addressSubject)
	require.NoError(t, modeRes[0].Err)

	time.Sleep(3 * time.Second)

	addressSchemaResp, err := src.CreateSchemaWithIDAndVersion(ctx, addressSubject, sr.Schema{
		Schema: addressSchema,
		Type:   sr.TypeAvro,
	}, 189, 1)
	require.NoError(t, err)
	t.Logf("Address schema created with ID: %d, version: %d", addressSchemaResp.ID, addressSchemaResp.Version)

	t.Log("And: person schema is created with reference to address schema with fixed ID")
	const (
		personSubject = "person01-value"
		personSchema  = `{"type":"record","name":"Person","namespace":"com.example.schemas","fields":[{"name":"id","type":"string"},{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"address","type":"com.example.schemas.Address"}]}`
	)

	t.Log("And: source and destination person subject is set to import mode")
	modeRes = src.SetMode(ctx, sr.ModeImport, personSubject)
	require.NoError(t, modeRes[0].Err)
	modeRes = dst.SetMode(ctx, sr.ModeImport, personSubject)
	require.NoError(t, modeRes[0].Err)

	time.Sleep(3 * time.Second)

	personSchemaResp, err := src.CreateSchemaWithIDAndVersion(ctx, personSubject, sr.Schema{
		Schema: personSchema,
		Type:   sr.TypeAvro,
		References: []sr.SchemaReference{
			{
				Name:    "com.example.schemas.Address",
				Subject: addressSubject,
				Version: addressSchemaResp.Version,
			},
		},
	}, 195, 1)
	require.NoError(t, err)
	t.Logf("Person schema created with ID: %d, version: %d", personSchemaResp.ID, personSchemaResp.Version)

	t.Log("When: migrator syncs schemas")
	conf := migrator.SchemaRegistryMigratorConfig{
		Enabled:  true,
		Versions: migrator.VersionsLatest,
	}
	m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)

	t.Log("When: migrator is run")
	ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
	defer cancel()
	require.NoError(t, m.Sync(ctx))

	t.Log("Then: address schema exists at destination with same ID")
	dstAddress, err := dst.SchemaByVersion(ctx, addressSubject, addressSchemaResp.Version)
	require.NoError(t, err)
	assert.Equal(t, addressSubject, dstAddress.Subject)
	assert.Equal(t, addressSchemaResp.ID, dstAddress.ID)
	assert.Equal(t, addressSchemaResp.Version, dstAddress.Version)
	assert.True(t, migrator.SchemaStringEquals(addressSchema, dstAddress.Schema.Schema, dstAddress.Type))

	t.Log("And: person schema exists at destination with same ID and reference")
	dstPerson, err := dst.SchemaByVersion(ctx, personSubject, personSchemaResp.Version)
	require.NoError(t, err)
	assert.Equal(t, personSubject, dstPerson.Subject)
	assert.Equal(t, personSchemaResp.ID, dstPerson.ID)
	assert.Equal(t, personSchemaResp.Version, dstPerson.Version)
	assert.True(t, migrator.SchemaStringEquals(personSchema, dstPerson.Schema.Schema, dstPerson.Type))

	t.Log("And: person schema has correct reference to address schema")
	require.Len(t, dstPerson.References, 1)
	ref := dstPerson.References[0]
	assert.Equal(t, "com.example.schemas.Address", ref.Name)
	assert.Equal(t, addressSubject, ref.Subject)
	assert.Equal(t, addressSchemaResp.Version, ref.Version)
}

func TestIntegrationSchemaRegistryMigratorSyncTranslateIDs(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: source and destination Schema Registry")
	src, dst := startSchemaRegistrySourceAndDestination(t)

	t.Log("And: destination pre-seed with a schema to take ID 1")
	_, err := dst.CreateSchema(t.Context(), "primed", sr.Schema{Schema: `{"type":"string"}`})
	require.NoError(t, err)

	t.Log("When: two schema versions exist at source")
	const subj = "foo"
	_, err = src.CreateSchema(t.Context(), subj, sr.Schema{Schema: dummyAvroSchemaV1})
	require.NoError(t, err)
	_, err = src.CreateSchema(t.Context(), subj, sr.Schema{Schema: dummyAvroSchemaV2})
	require.NoError(t, err)

	t.Log("And: migrator is configured to translate IDs")
	conf := migrator.SchemaRegistryMigratorConfig{
		Enabled:      true,
		Versions:     migrator.VersionsAll,
		TranslateIDs: true,
	}
	m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)

	t.Log("When: migrator is run")
	ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
	defer cancel()
	require.NoError(t, m.Sync(ctx))

	t.Log("Then: both versions exist at destination")
	sd1, err := dst.SchemaByVersion(ctx, subj, 1)
	require.NoError(t, err)
	sd2, err := dst.SchemaByVersion(ctx, subj, 2)
	require.NoError(t, err)
	assert.Greater(t, sd1.ID, 1)
	assert.Greater(t, sd2.ID, 1)
	assert.NotEqual(t, sd1.ID, sd2.ID)
}

func TestIntegrationSchemaRegistryMigratorSyncReuseIDs(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: source and destination Schema Registry")
	src, dst := startSchemaRegistrySourceAndDestination(t)

	const (
		schema1 = `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`
		schema2 = `{"type":"record","name":"Order","fields":[{"name":"orderId","type":"string"}]}`
	)

	t.Log("When: three subjects are created where two share identical schemas")
	ctx := t.Context()

	// Subject 1 and 2 have different schemas
	ss1, err := src.CreateSchema(ctx, "subject-1", sr.Schema{Schema: schema1})
	require.NoError(t, err)
	ss2, err := src.CreateSchema(ctx, "subject-2", sr.Schema{Schema: schema2})
	require.NoError(t, err)

	// Subject 3 shares the same schema as subject 1
	ss3, err := src.CreateSchema(ctx, "subject-3", sr.Schema{Schema: schema1 + "   "}) // Add trailing spaces to make it different
	require.NoError(t, err)

	t.Log("Then: subjects 1 and 3 should have the same schema ID")
	assert.Equal(t, ss1.ID, ss3.ID, "subject-1 and subject-3 should share schema ID")
	assert.NotEqual(t, ss1.ID, ss2.ID, "subject-1 and subject-2 should have different schema IDs")

	t.Log("When: destination is set to import mode")
	modeRes := dst.SetMode(ctx, sr.ModeImport)
	require.NoError(t, modeRes[0].Err)

	t.Log("And: migrator syncs schemas to destination")
	conf := migrator.SchemaRegistryMigratorConfig{
		Enabled:  true,
		Versions: migrator.VersionsLatest,
	}
	m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)

	syncCtx, cancel := context.WithTimeout(ctx, redpandaTestWaitTimeout)
	defer cancel()
	require.NoError(t, m.Sync(syncCtx))

	t.Log("Then: destination should have three subjects")
	subjects, err := dst.Subjects(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"subject-1", "subject-2", "subject-3"}, subjects)

	t.Log("And: destination subjects should preserve schema ID relationships")
	ds1, err := dst.SchemaByVersion(ctx, "subject-1", 1)
	require.NoError(t, err)
	ds2, err := dst.SchemaByVersion(ctx, "subject-2", 1)
	require.NoError(t, err)
	ds3, err := dst.SchemaByVersion(ctx, "subject-3", 1)
	require.NoError(t, err)

	assert.Equal(t, ds1.ID, ds3.ID, "destination subject-1 and subject-3 should share schema ID")
	assert.NotEqual(t, ds1.ID, ds2.ID, "destination subject-1 and subject-2 should have different schema IDs")

	t.Log("And: schema content should match source")
	assert.True(t, migrator.SchemaStringEquals(schema1, ds1.Schema.Schema, ds1.Type))
	assert.True(t, migrator.SchemaStringEquals(schema2, ds2.Schema.Schema, ds2.Type))
	assert.True(t, migrator.SchemaStringEquals(schema1, ds3.Schema.Schema, ds3.Type))
}

func TestIntegrationSchemaRegistryMigratorSyncNormalize(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: source and destination Schema Registry")
	src, dst := startSchemaRegistrySourceAndDestination(t)

	// Use Protobuf with fields out of order to exercise normalization at server
	t.Log("When: Protobuf schema with fields are out of order")
	const (
		subj = "pb"

		denorm = `syntax = "proto3";
package x;

message R {
  int32 a = 1;
  string c = 3;
  double b = 2;
}`

		norm = `syntax = "proto3";
package x;

message R {
  int32 a = 1;
  double b = 2;
  string c = 3;
}`
	)
	_, err := src.CreateSchema(t.Context(), subj, sr.Schema{Schema: denorm, Type: sr.TypeProtobuf})
	require.NoError(t, err)

	t.Log("And: destination is set to import mode")
	modeRes := dst.SetMode(t.Context(), sr.ModeImport)
	require.NoError(t, modeRes[0].Err)

	t.Log("And: migrator is configured to normalize")
	conf := migrator.SchemaRegistryMigratorConfig{
		Enabled:   true,
		Versions:  migrator.VersionsAll,
		Normalize: true,
	}
	m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)

	t.Log("When: migrator is run")
	ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
	defer cancel()
	require.NoError(t, m.Sync(ctx))

	t.Log("Then: normalized schema exists at destination")
	got, err := dst.SchemaByVersion(ctx, subj, 1)
	require.NoError(t, err)
	assert.Equal(t, sr.TypeProtobuf, got.Type)
	assert.True(t, migrator.SchemaStringEquals(norm, got.Schema.Schema, got.Type))
}

func TestIntegrationSchemaRegistryMigratorSyncIdempotence(t *testing.T) {
	integration.CheckSkip(t)

	tests := []struct {
		name      string
		translate bool
		mode      sr.Mode
	}{
		{name: "translate_ids=true", translate: true, mode: sr.ModeReadWrite},
		{name: "translate_ids=false", translate: false, mode: sr.ModeImport},
	}

	const subj = "idem"

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Log("Given: source and destination Schema Registry")
			src, dst := startSchemaRegistrySourceAndDestination(t)

			t.Log("When: two schema versions exist at source")
			_, err := src.CreateSchema(t.Context(), subj, sr.Schema{Schema: dummyAvroSchemaV1})
			require.NoError(t, err)
			_, err = src.CreateSchema(t.Context(), subj, sr.Schema{Schema: dummyAvroSchemaV2})
			require.NoError(t, err)

			t.Logf("And: destination is set to %s mode", tc.mode)
			modeRes := dst.SetMode(t.Context(), tc.mode)
			require.NoError(t, modeRes[0].Err)

			conf := migrator.SchemaRegistryMigratorConfig{
				Enabled:      true,
				Versions:     migrator.VersionsAll,
				TranslateIDs: tc.translate,
			}

			t.Log("When: migrator is run for the first time")
			ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
			defer cancel()
			m0 := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)
			require.NoError(t, m0.Sync(ctx))

			t.Log("Then: both versions exist at destination")
			vers, err := dst.SubjectVersions(ctx, subj)
			require.NoError(t, err)
			assert.ElementsMatch(t, vers, []int{1, 2})
			exp, err := dst.Schemas(ctx, subj)
			require.NoError(t, err)

			t.Log("When: migrator is run again")
			m1 := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)
			require.NoError(t, m1.Sync(ctx))

			t.Log("Then: no changes are made")
			got, err := dst.Schemas(ctx, subj)
			require.NoError(t, err)
			assert.Equal(t, exp, got)
		})
	}
}

func TestIntegrationSchemaRegistryMigratorCompatibilityFromSource(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: source and destination Schema Registry")
	src, dst := startSchemaRegistrySourceAndDestination(t)

	t.Log("And: a subject and schema exist at source")
	const (
		subj   = "compat-src"
		schema = `{"type":"string"}`
	)
	_, err := src.CreateSchema(t.Context(), subj, sr.Schema{Schema: schema})
	require.NoError(t, err)

	t.Log("And: source subject compatibility is set")
	level := sr.CompatFull
	set := src.SetCompatibility(t.Context(), sr.SetCompatibility{Level: level}, subj)
	require.NoError(t, set[0].Err)

	t.Log("And: destination is set to import mode")
	modeRes := dst.SetMode(t.Context(), sr.ModeImport)
	require.NoError(t, modeRes[0].Err)

	t.Log("When: migrator runs")
	conf := migrator.SchemaRegistryMigratorConfig{
		Enabled:  true,
		Versions: migrator.VersionsLatest,
	}
	m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)

	ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
	defer cancel()
	require.NoError(t, m.Sync(ctx))

	t.Log("Then: destination subject has same compatibility level")
	got := dst.Compatibility(ctx, subj)
	require.NoError(t, got[0].Err)
	assert.Equal(t, level, got[0].Level)
}

func TestIntegrationSchemaRegistryMigratorServerlessImportMode(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: source and destination Schema Registry")
	src, dst := startSchemaRegistrySourceAndDestination(t)

	t.Log("And: destination starts in READWRITE mode")
	ctx := t.Context()
	modeRes := dst.SetMode(ctx, sr.ModeReadWrite)
	require.NoError(t, modeRes[0].Err)

	t.Log("And: a schema exists at source")
	const (
		subj   = "serverless-test"
		schema = `{"type":"string"}`
	)
	_, err := src.CreateSchema(ctx, subj, sr.Schema{Schema: schema})
	require.NoError(t, err)

	t.Log("When: migrator runs in serverless mode")
	conf := migrator.SchemaRegistryMigratorConfig{
		Enabled:    true,
		Versions:   migrator.VersionsLatest,
		Serverless: true,
	}
	m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, dst)

	syncCtx, cancel := context.WithTimeout(ctx, redpandaTestWaitTimeout)
	defer cancel()
	require.NoError(t, m.Sync(syncCtx))

	t.Log("Then: schema exists at destination")
	dstSchema, err := dst.SchemaByVersion(ctx, subj, 1)
	require.NoError(t, err)
	assert.Equal(t, subj, dstSchema.Subject)
	assert.True(t, migrator.SchemaStringEquals(schema, dstSchema.Schema.Schema, dstSchema.Type))

	t.Log("And: destination mode is restored to READWRITE")
	mode := dst.Mode(ctx)
	require.NoError(t, mode[0].Err)
	assert.Equal(t, "READWRITE", mode[0].Mode.String())
}

func TestIntegrationSchemaRegistryMigratorDFS(t *testing.T) {
	integration.CheckSkip(t)

	src, _ := startSchemaRegistrySourceAndDestination(t)
	ctx := t.Context()

	t.Log("Setup: Create complex schema dependency graph with multiple versions")

	// Level 0: Base schemas with multiple versions
	base1v1 := `{"type":"record","name":"Base1","fields":[{"name":"id","type":"int"}]}`
	base1v2 := `{"type":"record","name":"Base1","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}`

	b1v1, err := src.CreateSchema(ctx, "base1", sr.Schema{Schema: base1v1, Type: sr.TypeAvro})
	require.NoError(t, err)
	b1v2, err := src.CreateSchema(ctx, "base1", sr.Schema{Schema: base1v2, Type: sr.TypeAvro})
	require.NoError(t, err)

	base2 := `{"type":"record","name":"Base2","fields":[{"name":"value","type":"string"}]}`
	b2v1, err := src.CreateSchema(ctx, "base2", sr.Schema{Schema: base2, Type: sr.TypeAvro})
	require.NoError(t, err)

	// Level 1: Mid schema references base1 v2 and base2
	mid1 := `{"type":"record","name":"Mid1","fields":[{"name":"b1","type":"Base1"},{"name":"b2","type":"Base2"}]}`
	m1v1, err := src.CreateSchema(ctx, "mid1", sr.Schema{
		Schema: mid1,
		Type:   sr.TypeAvro,
		References: []sr.SchemaReference{
			{Name: "Base1", Subject: "base1", Version: b1v2.Version},
			{Name: "Base2", Subject: "base2", Version: b2v1.Version},
		},
	})
	require.NoError(t, err)

	// Level 2: Top schema references mid1 and base1 v1
	top := `{"type":"record","name":"Top","fields":[{"name":"mid","type":"Mid1"},{"name":"oldBase","type":"Base1"}]}`
	topv1, err := src.CreateSchema(ctx, "top", sr.Schema{
		Schema: top,
		Type:   sr.TypeAvro,
		References: []sr.SchemaReference{
			{Name: "Mid1", Subject: "mid1", Version: m1v1.Version},
			{Name: "Base1", Subject: "base1", Version: b1v1.Version},
		},
	})
	require.NoError(t, err)

	t.Run("simple leaf traversal", func(t *testing.T) {
		t.Log("When: DFS starts from leaf schema (base2)")
		conf := migrator.SchemaRegistryMigratorConfig{
			Enabled:  true,
			Versions: migrator.VersionsLatest,
		}
		m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, nil)

		var traversed []string
		err = m.DfsSubjectSchemasFunc(ctx, src, b2v1, nil, func(schema sr.SubjectSchema) error {
			traversed = append(traversed, fmt.Sprintf("%s-v%d", schema.Subject, schema.Version))
			return nil
		})
		require.NoError(t, err)

		t.Log("Then: only single schema is traversed")
		assert.Equal(t, []string{"base2-v1"}, traversed)
	})

	t.Run("complex tree with VersionsAll", func(t *testing.T) {
		t.Log("When: DFS with VersionsAll starts from top")
		conf := migrator.SchemaRegistryMigratorConfig{
			Enabled:  true,
			Versions: migrator.VersionsAll,
		}
		m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, nil)

		var traversed []string
		err = m.DfsSubjectSchemasFunc(ctx, src, topv1, nil, func(schema sr.SubjectSchema) error {
			traversed = append(traversed, fmt.Sprintf("%s-v%d", schema.Subject, schema.Version))
			return nil
		})
		require.NoError(t, err)

		t.Log("Then: all schemas traversed with no duplicates")
		schemaCount := make(map[string]int)
		for _, s := range traversed {
			schemaCount[s]++
		}
		for schema, count := range schemaCount {
			assert.Equal(t, 1, count, "Schema %s visited exactly once", schema)
		}

		t.Log("And: all expected schemas present")
		expectedSchemas := map[string]bool{
			"top-v1": true, "mid1-v1": true,
			"base1-v1": true, "base1-v2": true, "base2-v1": true,
		}
		for _, s := range traversed {
			assert.True(t, expectedSchemas[s], "Unexpected schema: %s", s)
		}
		assert.Len(t, expectedSchemas, len(traversed))

		t.Log("And: dependencies processed before dependents")
		indices := make(map[string]int)
		for i, s := range traversed {
			indices[s] = i
		}
		assert.Less(t, indices["base1-v2"], indices["mid1-v1"])
		assert.Less(t, indices["base2-v1"], indices["mid1-v1"])
		assert.Less(t, indices["mid1-v1"], indices["top-v1"])
		assert.Less(t, indices["base1-v1"], indices["top-v1"])
	})

	t.Run("with filter", func(t *testing.T) {
		t.Log("When: DFS with filter excluding base2")
		conf := migrator.SchemaRegistryMigratorConfig{
			Enabled:  true,
			Versions: migrator.VersionsLatest,
		}
		m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, nil)

		filter := func(subject string, _ int) bool {
			return subject == "base2"
		}

		var traversed []string
		err = m.DfsSubjectSchemasFunc(ctx, src, m1v1, filter, func(schema sr.SubjectSchema) error {
			traversed = append(traversed, fmt.Sprintf("%s-v%d", schema.Subject, schema.Version))
			return nil
		})
		require.NoError(t, err)

		t.Log("Then: base2 not in results")
		for _, s := range traversed {
			assert.NotContains(t, s, "base2")
		}
		assert.Contains(t, traversed, "mid1-v1")
		assert.Contains(t, traversed, "base1-v2")
	})

	t.Run("callback error", func(t *testing.T) {
		t.Log("When: callback returns error")
		conf := migrator.SchemaRegistryMigratorConfig{
			Enabled:  true,
			Versions: migrator.VersionsLatest,
		}
		m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, src, nil)

		expectedErr := fmt.Errorf("test error")
		err = m.DfsSubjectSchemasFunc(ctx, src, b2v1, nil, func(_ sr.SubjectSchema) error {
			return expectedErr
		})

		t.Log("Then: error propagated")
		assert.ErrorIs(t, err, expectedErr)
	})
}
