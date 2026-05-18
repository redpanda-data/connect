// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/license"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type FakeFlightRecord struct {
	RealAddress faker.RealAddress `faker:"real_address"`
	CreatedAt   int64             `fake:"unix_time"`
}

func GetFakeFlightRecord() FakeFlightRecord {
	flightRecord := FakeFlightRecord{}
	err := faker.FakeData(&flightRecord)
	if err != nil {
		panic(err)
	}

	return flightRecord
}

func ResourceWithPostgreSQLVersion(t *testing.T, version string) (string, *sql.DB, error) {
	ctr, err := testcontainers.Run(t.Context(), "postgres:"+version,
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_PASSWORD": "l]YLSc|4[i56%{gY",
			"POSTGRES_USER":     "user_name",
			"POSTGRES_DB":       "dbname",
		}),
		testcontainers.WithCmd("postgres", "-c", "wal_level=logical"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	mp, err := ctr.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)

	password := "l]YLSc|4[i56%{gY"
	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, host, mp.Port())

	var db *sql.DB
	require.Eventually(t, func() bool {
		if db != nil {
			db.Close()
		}
		if db, err = sql.Open("postgres", databaseURL); err != nil {
			return false
		}

		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return false
		}

		var walLevel string
		if err = db.QueryRow("SHOW wal_level").Scan(&walLevel); err != nil {
			return false
		}

		var pgConfig string
		if err = db.QueryRow("SHOW config_file").Scan(&pgConfig); err != nil {
			return false
		}

		if walLevel != "logical" {
			return false
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS flights (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP);")
		if err != nil {
			return false
		}

		// Creating table with complex PG types
		_, err = db.Exec(`CREATE TABLE complex_types_example (
			id SERIAL PRIMARY KEY,
			json_data JSONB,
			tags TEXT[],
			ip_addr INET,
			search_text TSVECTOR,
			time_range TSRANGE,
			location POINT,
			uuid_col UUID,
			int_array INTEGER[]
		);`)
		if err != nil {
			return false
		}

		// This table explicitly uses identifiers that need quoting to ensure we work with those correctly.
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS "FlightsCompositePK" (
				"ID" serial, "Seq" integer, "Name" VARCHAR(50), "CreatedAt" TIMESTAMP,
				PRIMARY KEY ("ID", "Seq")
			);`)
		if err != nil {
			return false
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS large_values (id serial PRIMARY KEY, value TEXT);")
		if err != nil {
			return false
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS seq (id serial PRIMARY KEY);")
		if err != nil {
			return false
		}

		// flights_non_streamed is a control table with data that should not be streamed or queried by snapshot streaming
		_, err = db.Exec("CREATE TABLE IF NOT EXISTS flights_non_streamed (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP);")

		return err == nil
	}, 2*time.Minute, time.Second, "could not connect to postgres")
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})

	return databaseURL, db, nil
}

func TestIntegrationPostgresNoTxnMarkers(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	require.NoError(t, err)

	for i := range 10 {
		f := GetFakeFlightRecord()
		_, err = db.Exec(`INSERT INTO "FlightsCompositePK" ("Seq", "Name", "CreatedAt") VALUES ($1, $2, $3);`, i, f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	template := fmt.Sprintf(`
pg_stream:
    dsn: %s
    slot_name: test_slot_native_decoder
    stream_snapshot: true
    snapshot_batch_size: 5
    schema: public
    tables:
       - '"FlightsCompositePK"'
`, databaseURL)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		for _, msg := range mb {
			msgBytes, err := msg.AsBytes()
			require.NoError(t, err)
			outBatches = append(outBatches, string(msgBytes))
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(streamOut.Resources())

	go func() {
		_ = streamOut.Run(t.Context())
	}()

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10
	}, time.Second*25, time.Millisecond*100)

	for i := 10; i < 20; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec(`INSERT INTO "FlightsCompositePK" ("Seq", "Name", "CreatedAt") VALUES ($1, $2, $3);`, i, f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);`, f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		assert.Len(c, outBatches, 20, "got: %#v", outBatches)
	}, time.Second*25, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	// Starting stream for the same replication slot should continue from the last LSN
	// Meaning we must not receive any old messages again

	streamOutBuilder = service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	outBatches = []string{}
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		msgBytes, err := m.AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err = streamOutBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(streamOut.Resources())

	go func() {
		_ = streamOut.Run(t.Context())
	}()

	time.Sleep(time.Second * 5)
	for i := 20; i < 30; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec(`INSERT INTO "FlightsCompositePK" ("Seq", "Name", "CreatedAt") VALUES ($1, $2, $3);`, i, f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		assert.Len(c, outBatches, 10, "got: %#v", outBatches)
	}, time.Second*20, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationPgStreamingFromRemoteDB(t *testing.T) {
	t.Skip("This test requires a remote database to run. Aimed to test remote databases")

	// tables: users, products, orders, order_items

	template := `
pg_stream:
    dsn: postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable
    slot_name: test_slot_native_decoder
    snapshot_batch_size: 100000
    stream_snapshot: true
    include_transaction_markers: false
    temporary_slot: true
    schema: public
    tables:
       - users
       - products
       - orders
       - order_items
`

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outMessages int64
	var outMessagesMut sync.Mutex

	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		_, err := mb[0].AsBytes()
		require.NoError(t, err)
		outMessagesMut.Lock()
		outMessages += 1
		outMessagesMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(streamOut.Resources())

	go func() {
		_ = streamOut.Run(t.Context())
	}()

	assert.Eventually(t, func() bool {
		outMessagesMut.Lock()
		defer outMessagesMut.Unlock()
		return outMessages == 200000
	}, time.Minute*15, time.Millisecond*100)

	t.Log("Backfill conditioins are met 🎉")

	// you need to start inserting the data somewhere in another place
	time.Sleep(time.Minute * 30)
	outMessages = 0
	assert.Eventually(t, func() bool {
		outMessagesMut.Lock()
		defer outMessagesMut.Unlock()
		return outMessages == 1000000
	}, time.Minute*15, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationPostgresIncludeTxnMarkers(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	for range 10000 {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	template := fmt.Sprintf(`
pg_stream:
    dsn: %s
    slot_name: test_slot_native_decoder
    snapshot_batch_size: 100
    stream_snapshot: true
    include_transaction_markers: true
    schema: public
    tables:
       - flights
`, databaseURL)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		for _, msg := range mb {
			msgBytes, err := msg.AsBytes()
			require.NoError(t, err)
			outBatches = append(outBatches, string(msgBytes))
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(streamOut.Resources())

	go func() {
		err = streamOut.Run(t.Context())
		require.NoError(t, err)
	}()

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10000
	}, time.Second*25, time.Millisecond*100)

	for range 10 {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10030
	}, time.Second*25, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	// Starting stream for the same replication slot should continue from the last LSN
	// Meaning we must not receive any old messages again

	streamOutBuilder = service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	outBatches = []string{}
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		msgBytes, err := m.AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err = streamOutBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(streamOut.Resources())

	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			assert.NoError(t, err)
		}
	}()

	time.Sleep(time.Second * 5)
	for range 10 {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 30
	}, time.Second*20, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationPgCDCForPgOutputStreamComplexTypesPlugin(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	// inserting data
	_, err = db.Exec(`INSERT INTO complex_types_example (
		json_data,
		tags,
		ip_addr,
		search_text,
		time_range,
		location,
		uuid_col,
		int_array
	) VALUES (
		'{"name": "test", "value": 42}'::jsonb,
		ARRAY['tag1', 'tag2', 'tag3'],
		'192.168.1.1',
		to_tsvector('english', 'The quick brown fox jumps over the lazy dog'),
		tsrange('2024-01-01', '2024-12-31'),
		point(45.5, -122.6),
		'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
		ARRAY[1, 2, 3, 4, 5]
	);`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO complex_types_example (json_data) VALUES ('{"nested":null}'::jsonb);`)
	require.NoError(t, err)

	template := fmt.Sprintf(`
pg_stream:
    dsn: %s
    slot_name: test_slot_native_decoder
    snapshot_batch_size: 100
    stream_snapshot: true
    include_transaction_markers: false
    schema: public
    tables:
       - complex_types_example
`, databaseURL)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(streamOut.Resources())

	go func() {
		err = streamOut.Run(t.Context())
		require.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2
	}, time.Second*25, time.Millisecond*100)

	// producing change to non-complex type to trigger replication and receive updated row so we can check the complex types again
	// but after they have been produced by replication to ensure the consistency
	_, err = db.Exec("UPDATE complex_types_example SET id = 3 WHERE id = 1")
	require.NoError(t, err)
	_, err = db.Exec("UPDATE complex_types_example SET id = 4 WHERE id = 2")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 4
	}, time.Second*25, time.Millisecond*100)

	// replacing update with insert to remove replication messages type differences
	// so we will be checking only the data
	require.JSONEq(t, `{"id":1, "int_array":[1, 2, 3, 4, 5], "ip_addr":"192.168.1.1/32", "json_data":{"name":"test", "value":42}, "location": "(45.5,-122.6)", "search_text":"'brown':3 'dog':9 'fox':4 'jump':5 'lazi':8 'quick':2", "tags":["tag1", "tag2", "tag3"], "time_range": "[2024-01-01 00:00:00,2024-12-31 00:00:00)", "uuid_col":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}`, outBatches[0])
	require.JSONEq(t, `{"id":2, "int_array":null, "ip_addr":null, "json_data":{"nested":null}, "location":null, "search_text":null, "tags":null, "time_range":null, "uuid_col":null}`, outBatches[1])
	require.JSONEq(t, `{"id":3, "int_array":[1, 2, 3, 4, 5], "ip_addr":"192.168.1.1/32", "json_data":{"name":"test", "value":42}, "location": "(45.5,-122.6)", "search_text":"'brown':3 'dog':9 'fox':4 'jump':5 'lazi':8 'quick':2", "tags":["tag1", "tag2", "tag3"], "time_range": "[2024-01-01 00:00:00,2024-12-31 00:00:00)", "uuid_col":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}`, outBatches[2])
	require.JSONEq(t, `{"id":4, "int_array":null, "ip_addr":null, "json_data":{"nested":null}, "location":null, "search_text":null, "tags":null, "time_range":null, "uuid_col":null}`, outBatches[3])

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationMultiplePostgresVersions(t *testing.T) {
	integration.CheckSkip(t)
	// running tests in the look to test different PostgreSQL versions
	for _, version := range []string{"17", "16", "15", "14", "13", "12"} {
		v := version
		t.Run(version, func(t *testing.T) {
			t.Parallel()
			databaseURL, db, err := ResourceWithPostgreSQLVersion(t, v)
			require.NoError(t, err)
			require.NoError(t, err)

			for range 1000 {
				f := GetFakeFlightRecord()
				_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
				require.NoError(t, err)
			}

			template := fmt.Sprintf(`
pg_stream:
    dsn: %s
    slot_name: test_slot_native_decoder
    stream_snapshot: true
    include_transaction_markers: false
     # This is intentionally with uppercase - we want to validate
     # we treat identifiers the same as Postgres Queries.
    schema: PuBliC
    tables:
       # This is intentionally with uppercase - we want to validate
       # we treat identifiers the same as Postgres Queries.
       - FLIGHTS
`, databaseURL)

			streamOutBuilder := service.NewStreamBuilder()
			require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
			require.NoError(t, streamOutBuilder.AddInputYAML(template))

			var outBatches []string
			var outBatchMut sync.Mutex
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				for _, msg := range mb {
					msgBytes, err := msg.AsBytes()
					require.NoError(t, err)
					outBatches = append(outBatches, string(msgBytes))
				}
				return nil
			}))

			streamOut, err := streamOutBuilder.Build()
			require.NoError(t, err)

			license.InjectTestService(streamOut.Resources())

			go func() {
				_ = streamOut.Run(t.Context())
			}()

			assert.Eventually(t, func() bool {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				return len(outBatches) == 1000
			}, time.Minute, time.Millisecond*100)

			for range 1000 {
				f := GetFakeFlightRecord()
				_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
				require.NoError(t, err)
				_, err = db.Exec("INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
				require.NoError(t, err)
			}

			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				assert.Len(c, outBatches, 2000, "got: %d", len(outBatches))
			}, time.Minute, time.Millisecond*100)

			require.NoError(t, streamOut.StopWithin(time.Second*30))

			// Starting stream for the same replication slot should continue from the last LSN
			// Meaning we must not receive any old messages again

			streamOutBuilder = service.NewStreamBuilder()
			require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
			require.NoError(t, streamOutBuilder.AddInputYAML(template))

			outBatches = []string{}
			require.NoError(t, streamOutBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
				msgBytes, err := m.AsBytes()
				require.NoError(t, err)
				outBatchMut.Lock()
				outBatches = append(outBatches, string(msgBytes))
				outBatchMut.Unlock()
				return nil
			}))

			streamOut, err = streamOutBuilder.Build()
			require.NoError(t, err)

			license.InjectTestService(streamOut.Resources())

			go func() {
				if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			}()

			time.Sleep(time.Second * 5)
			for range 1000 {
				f := GetFakeFlightRecord()
				_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
				require.NoError(t, err)
			}

			// Postgres logical replication provides at-least-once delivery.
			// Upon reconnection to the same replication slot, a small number of
			// messages from the tail of the previous session may be replayed if
			// the final LSN ack did not reach Postgres before shutdown.
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				assert.GreaterOrEqual(c, len(outBatches), 1000, "expected at least 1000 messages, got: %d", len(outBatches))
			}, time.Minute, time.Millisecond*100)

			// Verify we didn't receive a large number of duplicate messages from
			// the previous session -- at most a handful may be replayed.
			outBatchMut.Lock()
			assert.LessOrEqual(t, len(outBatches), 1010, "too many duplicates replayed, got: %d", len(outBatches))
			outBatchMut.Unlock()

			require.NoError(t, streamOut.StopWithin(time.Second*30))
		})
	}
}

func TestIntegrationTOASTValues(t *testing.T) {
	integration.CheckSkip(t)

	for _, replicaIdentity := range []string{"FULL", "DEFAULT", "ALT_UNCHANGED_TOAST"} {
		t.Run(replicaIdentity, func(t *testing.T) {
			t.Parallel()
			databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
			require.NoError(t, err)

			if replicaIdentity == "FULL" {
				_, err = db.Exec(`ALTER TABLE large_values REPLICA IDENTITY FULL`)
				require.NoError(t, err)
			}

			const stringSize = 400_000

			require.NoError(t, err)

			// Insert a large >1MiB value
			_, err = db.Exec(`INSERT INTO large_values (id, value) VALUES ($1, $2);`, 1, strings.Repeat("foo", stringSize))
			require.NoError(t, err)

			template := strings.NewReplacer("$DSN", databaseURL).Replace(`
pg_stream:
    dsn: $DSN
    slot_name: test_slot_native_decoder
    stream_snapshot: true
    snapshot_batch_size: 1
    schema: public
    tables:
       - large_values
`)
			if replicaIdentity == "ALT_UNCHANGED_TOAST" {
				template += `
    unchanged_toast_value: '__redpanda_connect_unchanged_toast_yum__'
      `
			}

			streamOutBuilder := service.NewStreamBuilder()
			require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
			require.NoError(t, streamOutBuilder.AddInputYAML(template))

			var outBatches []string
			var outBatchMut sync.Mutex
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				for _, msg := range mb {
					msgBytes, err := msg.AsBytes()
					require.NoError(t, err)
					outBatches = append(outBatches, string(msgBytes))
				}
				return nil
			}))

			streamOut, err := streamOutBuilder.Build()
			require.NoError(t, err)

			license.InjectTestService(streamOut.Resources())

			go func() {
				_ = streamOut.Run(t.Context())
			}()

			assert.Eventually(t, func() bool {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				return len(outBatches) == 1
			}, time.Second*10, time.Millisecond*100)

			_, err = db.Exec(`UPDATE large_values SET value=$1;`, strings.Repeat("bar", stringSize))
			require.NoError(t, err)
			_, err = db.Exec(`UPDATE large_values SET id=$1;`, 3)
			require.NoError(t, err)
			_, err = db.Exec(`DELETE FROM large_values`)
			require.NoError(t, err)
			_, err = db.Exec(`INSERT INTO large_values (id, value) VALUES ($1, $2);`, 2, strings.Repeat("qux", stringSize))
			require.NoError(t, err)

			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				assert.Len(c, outBatches, 5, "got: %#v", outBatches)
			}, time.Second*10, time.Millisecond*100)
			require.JSONEq(t, `{"id":1, "value": "`+strings.Repeat("foo", stringSize)+`"}`, outBatches[0], "GOT: %s", outBatches[0])
			require.JSONEq(t, `{"id":1, "value": "`+strings.Repeat("bar", stringSize)+`"}`, outBatches[1], "GOT: %s", outBatches[1])
			switch replicaIdentity {
			case "FULL":
				require.JSONEq(t, `{"id":3, "value": "`+strings.Repeat("bar", stringSize)+`"}`, outBatches[2], "GOT: %s", outBatches[2])
				require.JSONEq(t, `{"id":3, "value": "`+strings.Repeat("bar", stringSize)+`"}`, outBatches[3], "GOT: %s", outBatches[3])
			case "DEFAULT":
				require.JSONEq(t, `{"id":3, "value": null}`, outBatches[2], "GOT: %s", outBatches[2])
				require.JSONEq(t, `{"id":3, "value": null}`, outBatches[3], "GOT: %s", outBatches[3])
			default:
				require.JSONEq(t, `{"id":3, "value": "__redpanda_connect_unchanged_toast_yum__"}`, outBatches[2], "GOT: %s", outBatches[2])
				require.JSONEq(t, `{"id":3, "value": null}`, outBatches[3], "GOT: %s", outBatches[3])
			}
			require.JSONEq(t, `{"id":2, "value": "`+strings.Repeat("qux", stringSize)+`"}`, outBatches[4], "GOT: %s", outBatches[4])

			require.NoError(t, streamOut.StopWithin(time.Second*10))
		})
	}
}

func TestIntegrationSnapshotConsistency(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	require.NoError(t, err)

	template := fmt.Sprintf(`
read_until:
  # Stop when we're idle for 3 seconds, which means our writer stopped
  idle_timeout: 3s
  input:
    pg_stream:
        dsn: %s
        slot_name: test_slot
        stream_snapshot: true
        snapshot_batch_size: 1
        schema: public
        tables:
           - seq
`, databaseURL)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var sequenceNumbers []int64
	var batchMu sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		batchMu.Lock()
		defer batchMu.Unlock()
		for _, msg := range batch {
			msg, err := msg.AsStructured()
			if err != nil {
				return err
			}
			seq, err := msg.(map[string]any)["id"].(json.Number).Int64()
			if err != nil {
				return err
			}
			sequenceNumbers = append(sequenceNumbers, seq)
		}
		return nil
	}))

	// Continuously write so there is a chance we skip data between snapshot and stream hand off.
	writer := asyncroutine.NewPeriodic(time.Microsecond, func() {
		_, err := db.Exec("INSERT INTO seq DEFAULT VALUES")
		require.NoError(t, err)
	})
	writer.Start()
	t.Cleanup(writer.Stop)

	// Wait to write some values so there are some values in the snapshot
	time.Sleep(10 * time.Millisecond)

	// Now start our stream
	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())
	streamStopped := make(chan any, 1)
	go func() {
		err = streamOut.Run(t.Context())
		require.NoError(t, err)
		streamStopped <- nil
	}()
	// Let the writer write a little more
	time.Sleep(5 * time.Second)
	writer.Stop()
	// Okay now wait for the stream to finish (the stream auto closes after it gets nothing for 3 seconds)
	select {
	case <-streamStopped:
	case <-time.After(30 * time.Second):
		require.Fail(t, "stream did not complete in time")
	}
	require.NoError(t, streamOut.StopWithin(10*time.Second))

	// Read the actual committed count from the database rather than
	// relying on the atomic counter, which can race with the last
	// INSERT commit.
	var dbCount int64
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM seq").Scan(&dbCount))

	expected := []int64{}
	for i := range dbCount {
		expected = append(expected, i+1)
	}
	batchMu.Lock()
	require.Equal(t, expected, sequenceNumbers)
	batchMu.Unlock()
}

func TestIntegrationSnapshotParallel(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	// Pre-insert rows into both tables so both pipelines have snapshot data.
	const numRows = 100
	for range numRows {
		_, err = db.Exec("INSERT INTO seq DEFAULT VALUES")
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO flights (name, created_at) VALUES ('test', NOW())`)
		require.NoError(t, err)
	}

	buildPipeline := func(slotName string) (*service.Stream, *[]int64, *sync.Mutex) {
		// max_parallel_snapshot_tables: 2 exercises the parallel errgroup scan path within
		// a single pipeline (two goroutines scanning seq and flights concurrently).
		// Running two such pipelines simultaneously exercises the concurrent-pipeline scenario
		// from the bug report.
		tmpl := fmt.Sprintf(`
read_until:
  idle_timeout: 5s
  input:
    postgres_cdc:
        dsn: %s
        slot_name: %s
        stream_snapshot: true
        snapshot_batch_size: 10
        max_parallel_snapshot_tables: 2
        schema: public
        tables:
          - seq
          - flights
`, databaseURL, slotName)

		builder := service.NewStreamBuilder()
		require.NoError(t, builder.SetLoggerYAML(`level: DEBUG`))
		require.NoError(t, builder.AddInputYAML(tmpl))

		var mu sync.Mutex
		var ids []int64
		require.NoError(t, builder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, msg := range batch {
				data, err := msg.AsStructured()
				if err != nil {
					return err
				}
				if id, ok := data.(map[string]any)["id"]; ok {
					n, err := id.(json.Number).Int64()
					if err != nil {
						return err
					}
					ids = append(ids, n)
				}
			}
			return nil
		}))

		stream, err := builder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())
		return stream, &ids, &mu
	}

	streamA, idsA, muA := buildPipeline("test_slot_parallel_a")
	streamB, idsB, muB := buildPipeline("test_slot_parallel_b")

	// Start both pipelines concurrently. With the bug, one or both will hang during
	// the snapshot phase: their scanTableRange goroutines block on s.messages <- batch
	// while holding open DB transactions, and the idle_timeout never fires because the
	// pipeline is not yet in the streaming phase. The test will time out.
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- streamA.Run(t.Context()) }()
	go func() { doneB <- streamB.Run(t.Context()) }()

	deadline := time.After(60 * time.Second)
	select {
	case err := <-doneA:
		require.NoError(t, err)
	case <-deadline:
		require.Fail(t, "pipeline A timed out - concurrent snapshot deadlock suspected")
	}
	select {
	case err := <-doneB:
		require.NoError(t, err)
	case <-deadline:
		require.Fail(t, "pipeline B timed out - concurrent snapshot deadlock suspected")
	}

	// Both pipelines should have received all rows from both tables.
	muA.Lock()
	assert.Len(t, *idsA, numRows*2, "pipeline A did not receive all rows from both tables")
	muA.Unlock()

	muB.Lock()
	assert.Len(t, *idsB, numRows*2, "pipeline B did not receive all rows from both tables")
	muB.Unlock()
}

func TestIntegrationPostgresMetadata(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO "FlightsCompositePK" ("Seq", "Name", "CreatedAt") VALUES ($1, $2, $3);`, 1, "delta", "2006-01-02T15:04:05Z07:00")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO flights (name, created_at) VALUES ($1, $2);`, "delta", "2006-01-02T15:04:05Z07:00")
	require.NoError(t, err)

	template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_native_decoder
    stream_snapshot: true
    snapshot_batch_size: 5
    schema: public
    tables:
      - '"FlightsCompositePK"'
      - flights
`, databaseURL)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))
	require.NoError(t, streamOutBuilder.AddProcessorYAML(`mapping: 'root = @'`))

	var outBatches []any
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		for _, msg := range batch {
			data, err := msg.AsStructured()
			require.NoError(t, err)
			d := data.(map[string]any)
			if _, ok := d["lsn"]; ok {
				d["lsn"] = "XXX/XXX" // Consistent LSN for assertions below
			}
			delete(d, "schema") // Schema metadata tested separately in TestIntegrationPostgresCDCSchemaMetadata
			outBatches = append(outBatches, data)
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(streamOut.Resources())

	go func() {
		_ = streamOut.Run(t.Context())
	}()

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2
	}, time.Second*25, time.Millisecond*100)

	_, err = db.Exec(`INSERT INTO "FlightsCompositePK" ("Seq", "Name", "CreatedAt") VALUES ($1, $2, $3);`, 2, "bravo", "2006-01-02T15:04:05Z07:00")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO flights (name, created_at) VALUES ($1, $2);`, "bravo", "2006-01-02T15:04:05Z07:00")
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		assert.Len(c, outBatches, 4, "got: %#v", outBatches)
	}, time.Second*25, time.Millisecond*100)

	require.ElementsMatch(
		t,
		outBatches,
		[]any{
			map[string]any{
				"operation": "read",
				"table":     "FlightsCompositePK",
			},
			map[string]any{
				"operation": "read",
				"table":     "flights",
			},
			map[string]any{
				"operation": "insert",
				"table":     "flights",
				"lsn":       "XXX/XXX",
			},
			map[string]any{
				"operation": "insert",
				"table":     "FlightsCompositePK",
				"lsn":       "XXX/XXX",
			},
		},
	)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationHeartbeat(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	require.NoError(t, err)

	template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_native_decoder
    schema: public
    heartbeat_interval: 1s
    pg_standby_timeout: 1s
    tables:
      - seq
`, databaseURL)

	writer := asyncroutine.NewPeriodic(time.Millisecond, func() {
		_, err := db.Exec("INSERT INTO seq DEFAULT VALUES")
		require.NoError(t, err)
	})
	writer.Start()
	t.Cleanup(writer.Stop)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))
	recvCount := &atomic.Int64{}
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(context.Context, service.MessageBatch) error {
		recvCount.Add(1)
		return nil
	}))
	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())
	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	// Wait for replication slot to be created
	t.Log("Waiting for replication slot to be created")
	require.Eventually(t, func() bool {
		rows, err := db.Query("SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'test_slot_native_decoder'")
		if err != nil {
			t.Logf("Error querying replication slots: %v", err)
			return false
		}
		defer rows.Close()
		require.NoError(t, rows.Err())

		exists := rows.Next()
		if exists {
			t.Log("Replication slot 'test_slot_native_decoder' has been created")
		}
		return exists
	}, 10*time.Second, 500*time.Millisecond, "replication slot was not created in time")

	getRestartLSN := func() string {
		rows, err := db.Query("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'test_slot_native_decoder'")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var lsn string
			require.NoError(t, rows.Scan(&lsn))
			return lsn
		}
		require.NoError(t, rows.Err())
		require.FailNow(t, "unable to get replication slot position")
		return ""
	}

	// Make sure the LSN advances even when no messages are being emitted (via heartbeat)
	startLSN := getRestartLSN()
	t.Logf("Initial confirmed_flush_lsn: %s", startLSN)
	require.Eventually(t, func() bool {
		currentLSN := getRestartLSN()
		t.Logf("Current confirmed_flush_lsn: %s, start: %s", currentLSN, startLSN)
		return currentLSN > startLSN
	}, 10*time.Second, 500*time.Millisecond, "LSN did not advance within timeout")

	t.Log("LSN successfully advanced, stopping stream")
	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationPostgresCDCSchemaMetadata(t *testing.T) {
	integration.CheckSkip(t)

	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	// Create a table that exercises every distinct type mapping in pgTypeNameToCommonType,
	// plus INET as a representative unknown type whose schema falls back to ANY.
	_, err = db.Exec(`CREATE TABLE schema_test_table (
		id              SERIAL PRIMARY KEY,
		col_bool        BOOLEAN,
		col_smallint    SMALLINT,
		col_int         INTEGER,
		col_bigint      BIGINT,
		col_float4      REAL,
		col_float8      DOUBLE PRECISION,
		col_numeric     NUMERIC(10,2),
		col_text        TEXT,
		col_varchar     VARCHAR(100),
		col_char        CHAR(10),
		col_bytea       BYTEA,
		col_date        DATE,
		col_time        TIME,
		col_timetz      TIMETZ,
		col_timestamp   TIMESTAMP,
		col_timestamptz TIMESTAMPTZ,
		col_json        JSON,
		col_jsonb       JSONB,
		col_uuid        UUID,
		col_inet        INET
	)`)
	require.NoError(t, err)

	// Insert two rows before starting the stream so they arrive as snapshot reads.
	_, err = db.Exec(`INSERT INTO schema_test_table
		(col_bool, col_smallint, col_int, col_bigint, col_float4, col_float8,
		 col_numeric, col_text, col_varchar, col_char, col_bytea, col_date,
		 col_time, col_timetz, col_timestamp, col_timestamptz, col_json, col_jsonb,
		 col_uuid, col_inet)
		VALUES
		(TRUE,  1, 10, 1000000000, 1.5, 3.14, 123.45, 'alice', 'hello', 'hi',
		 '\x48656c6c6f', '2024-01-15', '10:00:00', '10:00:00+00',
		 '2024-01-15 10:00:00', '2024-01-15 10:00:00+00',
		 '{"k":1}', '{"k":1}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '10.0.0.1'),
		(FALSE, 2, 20, 2000000000, 2.5, 6.28, 456.78, 'bob',   'world', 'bye',
		 '\x576f726c64', '2024-06-01', '20:00:00', '20:00:00+00',
		 '2024-06-01 20:00:00', '2024-06-01 20:00:00+00',
		 '{"k":2}', '{"k":2}', 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22', '10.0.0.2')`)
	require.NoError(t, err)

	type collectedMsg struct {
		operation string
		table     string
		lsn       string
		hasSchema bool
		schema    map[string]any
	}

	var (
		mu       sync.Mutex
		messages []collectedMsg
	)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetLoggerYAML(`level: WARN`))
	require.NoError(t, sb.AddInputYAML(fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: schema_test_slot
    stream_snapshot: true
    snapshot_batch_size: 10
    schema: public
    tables:
      - schema_test_table
`, databaseURL)))

	require.NoError(t, sb.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range batch {
			cm := collectedMsg{}
			cm.operation, _ = msg.MetaGet("operation")
			cm.table, _ = msg.MetaGet("table")
			cm.lsn, _ = msg.MetaGet("lsn")
			_ = msg.MetaWalkMut(func(key string, value any) error {
				if key == "schema" {
					if m, ok := value.(map[string]any); ok {
						cm.hasSchema = true
						cm.schema = m
					}
				}
				return nil
			})
			messages = append(messages, cm)
		}
		return nil
	}))

	streamOut, err := sb.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())

	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	t.Cleanup(func() {
		require.NoError(t, streamOut.StopWithin(10*time.Second))
	})

	// --- Phase 1: snapshot + CDC schema check ---

	// Wait for 2 snapshot rows.
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(messages) >= 2
	}, 30*time.Second, 100*time.Millisecond)

	// Insert 2 CDC rows.
	_, err = db.Exec(`INSERT INTO schema_test_table
		(col_bool, col_smallint, col_int, col_bigint, col_float4, col_float8,
		 col_numeric, col_text, col_varchar, col_char, col_bytea, col_date,
		 col_time, col_timetz, col_timestamp, col_timestamptz, col_json, col_jsonb,
		 col_uuid, col_inet)
		VALUES
		(TRUE,  3, 30, 3000000000, 3.5, 9.42, 789.01, 'carol', 'foo', 'cat',
		 '\x466f6f', '2024-09-01', '09:00:00', '09:00:00+00',
		 '2024-09-01 09:00:00', '2024-09-01 09:00:00+00',
		 '{"k":3}', '{"k":3}', 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33', '10.0.0.3'),
		(FALSE, 4, 40, 4000000000, 4.5, 12.56, 111.22, 'dave', 'bar', 'dog',
		 '\x426172', '2024-12-01', '15:00:00', '15:00:00+00',
		 '2024-12-01 15:00:00', '2024-12-01 15:00:00+00',
		 '{"k":4}', '{"k":4}', 'd0eebc99-9c0b-4ef8-bb6d-6bb9bd380a44', '10.0.0.4')`)
	require.NoError(t, err)

	// Wait for all 4 messages.
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(messages) >= 4
	}, 30*time.Second, 100*time.Millisecond)

	mu.Lock()
	phase1 := make([]collectedMsg, 4)
	copy(phase1, messages)
	mu.Unlock()

	// verifySchemaAllCols checks all 21 columns against their expected schema types.
	verifySchemaAllCols := func(t *testing.T, schema map[string]any) {
		t.Helper()
		require.NotNil(t, schema)
		assert.Equal(t, "schema_test_table", schema["name"])
		assert.Equal(t, "OBJECT", schema["type"])

		rawChildren, ok := schema["children"]
		require.True(t, ok, "schema must have a children key")
		children, ok := rawChildren.([]any)
		require.True(t, ok, "children must be []any")
		assert.Len(t, children, 21)

		byName := make(map[string]string, len(children))
		for _, c := range children {
			child := c.(map[string]any)
			byName[child["name"].(string)] = child["type"].(string)
		}
		assert.Equal(t, "INT32", byName["id"])
		assert.Equal(t, "BOOLEAN", byName["col_bool"], "BOOLEAN column")
		assert.Equal(t, "INT32", byName["col_smallint"], "SMALLINT column")
		assert.Equal(t, "INT32", byName["col_int"], "INTEGER column")
		assert.Equal(t, "INT64", byName["col_bigint"], "BIGINT column")
		assert.Equal(t, "FLOAT32", byName["col_float4"], "REAL column")
		assert.Equal(t, "FLOAT64", byName["col_float8"], "DOUBLE PRECISION column")
		assert.Equal(t, "DECIMAL", byName["col_numeric"], "NUMERIC column")
		assert.Equal(t, "STRING", byName["col_text"], "TEXT column")
		assert.Equal(t, "STRING", byName["col_varchar"], "VARCHAR column")
		assert.Equal(t, "STRING", byName["col_char"], "CHAR column")
		assert.Equal(t, "BYTE_ARRAY", byName["col_bytea"], "BYTEA column")
		assert.Equal(t, "TIMESTAMP", byName["col_date"], "DATE column")
		assert.Equal(t, "STRING", byName["col_time"], "TIME column")
		assert.Equal(t, "STRING", byName["col_timetz"], "TIMETZ column")
		assert.Equal(t, "TIMESTAMP", byName["col_timestamp"], "TIMESTAMP column")
		assert.Equal(t, "TIMESTAMP", byName["col_timestamptz"], "TIMESTAMPTZ column")
		assert.Equal(t, "ANY", byName["col_json"], "JSON column")
		assert.Equal(t, "ANY", byName["col_jsonb"], "JSONB column")
		assert.Equal(t, "STRING", byName["col_uuid"], "UUID column")
		assert.Equal(t, "ANY", byName["col_inet"], "INET (unknown type) column")
	}

	// Snapshot messages: operation=read, no lsn, schema present.
	for i, cm := range phase1[:2] {
		assert.Equal(t, "read", cm.operation, "snapshot msg %d: wrong operation", i)
		assert.Equal(t, "schema_test_table", cm.table)
		assert.Empty(t, cm.lsn, "snapshot msg %d: should have no lsn", i)
		assert.True(t, cm.hasSchema, "snapshot msg %d: missing schema metadata", i)
		verifySchemaAllCols(t, cm.schema)
	}

	// CDC messages: operation=insert, lsn set, schema present.
	for i, cm := range phase1[2:] {
		assert.Equal(t, "insert", cm.operation, "cdc msg %d: wrong operation", i)
		assert.Equal(t, "schema_test_table", cm.table)
		assert.NotEmpty(t, cm.lsn, "cdc msg %d: should have an lsn", i)
		assert.True(t, cm.hasSchema, "cdc msg %d: missing schema metadata", i)
		verifySchemaAllCols(t, cm.schema)
	}

	// --- Phase 2: DDL change invalidates the schema cache ---

	_, err = db.Exec(`ALTER TABLE schema_test_table ADD COLUMN extra TEXT`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO schema_test_table
		(col_bool, col_smallint, col_int, col_bigint, col_float4, col_float8,
		 col_numeric, col_text, col_varchar, col_char, col_bytea, col_date,
		 col_time, col_timetz, col_timestamp, col_timestamptz, col_json, col_jsonb,
		 col_uuid, col_inet, extra)
		VALUES
		(TRUE, 5, 50, 5000000000, 5.5, 15.70, 222.33, 'eve', 'baz', 'elk',
		 '\x42617a', '2025-01-01', '08:00:00', '08:00:00+00',
		 '2025-01-01 08:00:00', '2025-01-01 08:00:00+00',
		 '{"k":5}', '{"k":5}', 'e0eebc99-9c0b-4ef8-bb6d-6bb9bd380a55', '10.0.0.5',
		 'bonus')`)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(messages) >= 5
	}, 30*time.Second, 100*time.Millisecond)

	mu.Lock()
	fifth := messages[4]
	mu.Unlock()

	assert.Equal(t, "insert", fifth.operation)
	assert.NotEmpty(t, fifth.lsn)
	assert.True(t, fifth.hasSchema, "post-ALTER CDC message must have schema metadata")

	rawChildren, ok := fifth.schema["children"]
	require.True(t, ok, "post-ALTER schema must have children")
	children := rawChildren.([]any)
	assert.Len(t, children, 22, "post-ALTER schema should reflect the new column")

	byName := make(map[string]string, len(children))
	for _, c := range children {
		child := c.(map[string]any)
		byName[child["name"].(string)] = child["type"].(string)
	}
	assert.Equal(t, "STRING", byName["extra"], "new 'extra' column should have type STRING")
}
