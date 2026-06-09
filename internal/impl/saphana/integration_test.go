// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

const (
	hanaImage    = "saplabs/hanaexpress:latest"
	hanaPort     = "39017/tcp"
	hanaUser     = "SYSTEM"
	hanaPassword = "HXEHana@1"
)

func startHANA(t *testing.T) string {
	t.Helper()
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(
		t.Context(),
		hanaImage,
		testcontainers.WithExposedPorts(hanaPort),
		testcontainers.WithEnv(map[string]string{
			"AGREE_TO_SAP_LICENSE": "Y",
			"MASTER_PASSWORD":      hanaPassword,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort(hanaPort).WithStartupTimeout(5*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	port, err := ctr.MappedPort(t.Context(), hanaPort)
	require.NoError(t, err)

	dsn := fmt.Sprintf("hdb://%s:%s@%s:%s", hanaUser, hanaPassword, host, port.Port())

	db, err := sql.Open("hdb", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.Eventually(t, func() bool {
		return db.PingContext(t.Context()) == nil
	}, 5*time.Minute, 5*time.Second, "HANA did not become ready in time")

	return dsn
}

func openTestDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("hdb", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// readAllMessages connects and drains the input until ErrEndOfInput or context timeout.
func readAllMessages(t *testing.T, dsn, yaml string) []map[string]any {
	t.Helper()

	conf, err := sapHANAInputConfigSpec.ParseYAML(yaml, nil)
	require.NoError(t, err)

	input, err := newSAPHANAInput(conf, enterpriseResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = input.Close(context.Background()) })

	require.NoError(t, input.Connect(t.Context()))

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]any
	for {
		batch, ack, err := input.ReadBatch(ctx)
		if err != nil {
			break
		}
		require.NoError(t, ack(context.Background(), nil))

		for _, msg := range batch {
			raw, err := msg.AsBytes()
			require.NoError(t, err)

			var row map[string]any
			require.NoError(t, json.Unmarshal(raw, &row))
			results = append(results, row)
		}
	}
	return results
}

func TestIntegrationSAPHANAInputBulk(t *testing.T) {
	integration.CheckSkip(t)
	t.Log("Given a HANA instance with a table containing 3 rows")
	dsn := startHANA(t)
	db := openTestDB(t, dsn)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE BULK_TEST (ID INTEGER, NAME NVARCHAR(100))`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE BULK_TEST`) })

	for i := 1; i <= 3; i++ {
		_, err := db.ExecContext(t.Context(),
			`INSERT INTO BULK_TEST (ID, NAME) VALUES (?, ?)`, i, fmt.Sprintf("row-%d", i))
		require.NoError(t, err)
	}

	t.Log("When the input reads BULK_TEST in bulk mode")
	rows := readAllMessages(t, dsn, fmt.Sprintf(`
dsn: %q
mode: bulk
table: BULK_TEST
`, dsn))

	t.Log("Then all 3 rows are returned")
	require.Len(t, rows, 3)
	ids := make([]int64, 0, 3)
	for _, row := range rows {
		switch v := row["ID"].(type) {
		case float64:
			ids = append(ids, int64(v))
		case int64:
			ids = append(ids, v)
		default:
			t.Fatalf("unexpected ID type %T", row["ID"])
		}
	}
	assert.ElementsMatch(t, []int64{1, 2, 3}, ids)
}

func TestIntegrationSAPHANAInputQuery(t *testing.T) {
	integration.CheckSkip(t)
	t.Log("Given a HANA table with 4 rows, 2 active and 2 inactive")
	dsn := startHANA(t)
	db := openTestDB(t, dsn)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE QUERY_TEST (ID INTEGER, ACTIVE TINYINT)`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE QUERY_TEST`) })

	for i, active := range []int{1, 0, 1, 0} {
		_, err := db.ExecContext(t.Context(),
			`INSERT INTO QUERY_TEST (ID, ACTIVE) VALUES (?, ?)`, i+1, active)
		require.NoError(t, err)
	}

	t.Log("When the input reads with a WHERE ACTIVE = 1 filter in query mode")
	rows := readAllMessages(t, dsn, fmt.Sprintf(`
dsn: %q
mode: query
query: "SELECT * FROM QUERY_TEST WHERE ACTIVE = 1"
`, dsn))

	t.Log("Then only the 2 active rows are returned")
	require.Len(t, rows, 2)
	for _, row := range rows {
		switch v := row["ACTIVE"].(type) {
		case float64:
			assert.Equal(t, float64(1), v)
		case int64:
			assert.Equal(t, int64(1), v)
		default:
			t.Fatalf("unexpected ACTIVE type %T", row["ACTIVE"])
		}
	}
}

func TestIntegrationSAPHANAInputIncrementing(t *testing.T) {
	integration.CheckSkip(t)
	t.Log("Given a HANA table with 5 rows keyed by a monotonic ID column")
	dsn := startHANA(t)
	db := openTestDB(t, dsn)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE INC_TEST (ID BIGINT, VAL NVARCHAR(50))`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE INC_TEST`) })

	for i := int64(1); i <= 5; i++ {
		_, err := db.ExecContext(t.Context(),
			`INSERT INTO INC_TEST (ID, VAL) VALUES (?, ?)`, i, fmt.Sprintf("v-%d", i))
		require.NoError(t, err)
	}

	confYAML := fmt.Sprintf(`
dsn: %q
mode: incrementing
table: INC_TEST
incrementing_column: ID
poll_interval: 100ms
`, dsn)

	t.Log("When the input reads in incrementing mode with no initial HWM")
	rows := readAllMessages(t, dsn, confYAML)
	t.Log("Then all 5 rows are returned")
	require.Len(t, rows, 5)

	confWithHWM := fmt.Sprintf(`
dsn: %q
mode: incrementing
table: INC_TEST
incrementing_column: ID
incrementing_initial_value: "3"
poll_interval: 100ms
`, dsn)

	t.Log("When the input reads in incrementing mode with initial HWM=3")
	rows2 := readAllMessages(t, dsn, confWithHWM)
	t.Log("Then only rows with ID > 3 are returned")
	require.Len(t, rows2, 2)
	for _, row := range rows2 {
		var id int64
		switch v := row["ID"].(type) {
		case float64:
			id = int64(v)
		case int64:
			id = v
		default:
			t.Fatalf("unexpected ID type %T", row["ID"])
		}
		assert.Greater(t, id, int64(3))
	}
}

func TestIntegrationSAPHANASchemaMetadata(t *testing.T) {
	integration.CheckSkip(t)
	t.Log("Given a HANA table with one row and schema_name configured")
	dsn := startHANA(t)
	db := openTestDB(t, dsn)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE SCHEMA_TEST (ID INTEGER, NAME NVARCHAR(50))`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE SCHEMA_TEST`) })

	_, err = db.ExecContext(t.Context(), `INSERT INTO SCHEMA_TEST (ID, NAME) VALUES (1, 'alice')`)
	require.NoError(t, err)

	conf, err := sapHANAInputConfigSpec.ParseYAML(fmt.Sprintf(`
dsn: %q
mode: bulk
table: SCHEMA_TEST
schema_name: SYSTEM
`, dsn), nil)
	require.NoError(t, err)

	input, err := newSAPHANAInput(conf, enterpriseResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = input.Close(context.Background()) })

	t.Log("When the input reads the row")
	require.NoError(t, input.Connect(t.Context()))

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	batch, ack, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.NoError(t, ack(context.Background(), nil))
	require.NotEmpty(t, batch)

	t.Log("Then the message carries a 'schema' metadata field derived from SYS.TABLE_COLUMNS")
	schemaVal, ok := batch[0].MetaGet("schema")
	assert.True(t, ok, "expected 'schema' metadata key")
	assert.NotNil(t, schemaVal)
}

func TestIntegrationSAPHANAOutputWriteBatch(t *testing.T) {
	integration.CheckSkip(t)
	t.Log("Given an empty HANA table and an output configured for bulk insert")
	dsn := startHANA(t)
	db := openTestDB(t, dsn)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE OUT_WRITE_TEST (ID INTEGER, NAME NVARCHAR(100))`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE OUT_WRITE_TEST`) })

	conf, err := sapHANAOutputConfigSpec.ParseYAML(fmt.Sprintf(`
dsn: %q
table: OUT_WRITE_TEST
columns: [ID, NAME]
args_mapping: 'root = [this.id, this.name]'
`, dsn), nil)
	require.NoError(t, err)

	out, err := newSAPHANAOutput(conf, enterpriseResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close(context.Background()) })
	require.NoError(t, out.Connect(t.Context()))

	t.Log("When WriteBatch is called with 3 messages")
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":1,"name":"alice"}`)),
		service.NewMessage([]byte(`{"id":2,"name":"bob"}`)),
		service.NewMessage([]byte(`{"id":3,"name":"carol"}`)),
	}
	require.NoError(t, out.WriteBatch(t.Context(), batch))

	t.Log("Then all 3 rows appear in the table in order")
	rows, err := db.QueryContext(t.Context(), `SELECT ID, NAME FROM OUT_WRITE_TEST ORDER BY ID`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		id   int64
		name string
	}
	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.name))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 3)
	assert.Equal(t, row{1, "alice"}, got[0])
	assert.Equal(t, row{2, "bob"}, got[1])
	assert.Equal(t, row{3, "carol"}, got[2])
}

func TestIntegrationSAPHANAOutputMultipleBatches(t *testing.T) {
	integration.CheckSkip(t)
	t.Log("Given an empty HANA table and an output configured for bulk insert")
	dsn := startHANA(t)
	db := openTestDB(t, dsn)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE OUT_MULTI_TEST (ID INTEGER, NAME NVARCHAR(100))`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE OUT_MULTI_TEST`) })

	conf, err := sapHANAOutputConfigSpec.ParseYAML(fmt.Sprintf(`
dsn: %q
table: OUT_MULTI_TEST
columns: [ID, NAME]
args_mapping: 'root = [this.id, this.name]'
`, dsn), nil)
	require.NoError(t, err)

	out, err := newSAPHANAOutput(conf, enterpriseResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close(context.Background()) })
	require.NoError(t, out.Connect(t.Context()))

	t.Log("When WriteBatch is called twice with 2 messages each")
	require.NoError(t, out.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":1,"name":"alice"}`)),
		service.NewMessage([]byte(`{"id":2,"name":"bob"}`)),
	}))
	require.NoError(t, out.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":3,"name":"carol"}`)),
		service.NewMessage([]byte(`{"id":4,"name":"dave"}`)),
	}))

	t.Log("Then all 4 rows are present in the table")
	var count int64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM OUT_MULTI_TEST`).Scan(&count))
	assert.Equal(t, int64(4), count)
}

func TestIntegrationSAPHANAOutputArgsMappingError(t *testing.T) {
	integration.CheckSkip(t)
	t.Log("Given an output whose args_mapping returns a string instead of an array")
	dsn := startHANA(t)
	db := openTestDB(t, dsn)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE OUT_ERR_TEST (ID INTEGER, NAME NVARCHAR(100))`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE OUT_ERR_TEST`) })

	conf, err := sapHANAOutputConfigSpec.ParseYAML(fmt.Sprintf(`
dsn: %q
table: OUT_ERR_TEST
columns: [ID, NAME]
args_mapping: 'root = "not-an-array"'
`, dsn), nil)
	require.NoError(t, err)

	out, err := newSAPHANAOutput(conf, enterpriseResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close(context.Background()) })
	require.NoError(t, out.Connect(t.Context()))

	t.Log("When WriteBatch is called")
	err = out.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":1,"name":"alice"}`)),
	})
	t.Log("Then an error is returned indicating the mapping must produce an array")
	require.ErrorContains(t, err, "must return an array")
}
