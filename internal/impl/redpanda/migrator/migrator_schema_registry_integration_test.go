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
	"regexp"
	"testing"

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
	}{
		{name: "translate_ids=true", translate: true},
		{name: "translate_ids=false", translate: false},
	}

	const subj = "idem"

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			t.Log("Given: source and destination Schema Registry")
			src, dst := startSchemaRegistrySourceAndDestination(t)

			t.Log("When: two schema versions exist at source")
			_, err := src.CreateSchema(t.Context(), subj, sr.Schema{Schema: dummyAvroSchemaV1})
			require.NoError(t, err)
			_, err = src.CreateSchema(t.Context(), subj, sr.Schema{Schema: dummyAvroSchemaV2})
			require.NoError(t, err)

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
