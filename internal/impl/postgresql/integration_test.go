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

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
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

func ResourceWithPostgreSQLVersion(t *testing.T, pool *dockertest.Pool, version string) (*dockertest.Resource, *sql.DB, error) {
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        version,
		Env: []string{
			"POSTGRES_PASSWORD=l]YLSc|4[i56%{gY",
			"POSTGRES_USER=user_name",
			"POSTGRES_DB=dbname",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})

	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"
	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])

	var db *sql.DB
	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		if db, err = sql.Open("postgres", databaseURL); err != nil {
			return err
		}

		t.Cleanup(func() {
			_ = db.Close()
		})

		if err = db.Ping(); err != nil {
			return err
		}

		var walLevel string
		if err = db.QueryRow("SHOW wal_level").Scan(&walLevel); err != nil {
			return err
		}

		var pgConfig string
		if err = db.QueryRow("SHOW config_file").Scan(&pgConfig); err != nil {
			return err
		}

		if walLevel != "logical" {
			return fmt.Errorf("wal_level is not logical")
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS flights (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP);")
		if err != nil {
			return err
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
			return err
		}

		// This table explicitly uses identifiers that need quoting to ensure we work with those correctly.
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS "FlightsCompositePK" (
				"ID" serial, "Seq" integer, "Name" VARCHAR(50), "CreatedAt" TIMESTAMP,
				PRIMARY KEY ("ID", "Seq")
			);`)
		if err != nil {
			return err
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS large_values (id serial PRIMARY KEY, value TEXT);")
		if err != nil {
			return err
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS seq (id serial PRIMARY KEY);")
		if err != nil {
			return err
		}

		// flights_non_streamed is a control table with data that should not be streamed or queried by snapshot streaming
		_, err = db.Exec("CREATE TABLE IF NOT EXISTS flights_non_streamed (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP);")

		return err
	}); err != nil {
		panic(fmt.Errorf("could not connect to docker: %w", err))
	}

	return resource, db, nil
}

func TestIntegrationPostgresNoTxnMarkers(t *testing.T) {
	t.Parallel()
	integration.CheckSkip(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		resource *dockertest.Resource
		db       *sql.DB
	)

	resource, db, err = ResourceWithPostgreSQLVersion(t, pool, "16")
	require.NoError(t, err)
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"

	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec(`INSERT INTO "FlightsCompositePK" ("Seq", "Name", "CreatedAt") VALUES ($1, $2, $3);`, i, f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
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
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: TRACE`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
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
		assert.NoError(t, streamOut.Run(t.Context()))
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
	t.Parallel()
	integration.CheckSkip(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		resource *dockertest.Resource
		db       *sql.DB
	)

	resource, db, err = ResourceWithPostgreSQLVersion(t, pool, "16")
	require.NoError(t, err)
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"

	for i := 0; i < 10000; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
	}

	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
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
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: TRACE`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
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

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10000
	}, time.Second*25, time.Millisecond*100)

	for i := 0; i < 10; i++ {
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
		assert.NoError(t, streamOut.Run(t.Context()))
	}()

	time.Sleep(time.Second * 5)
	for i := 0; i < 10; i++ {
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
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		resource *dockertest.Resource
		db       *sql.DB
	)

	resource, db, err = ResourceWithPostgreSQLVersion(t, pool, "16")
	require.NoError(t, err)
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"

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

	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
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
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: TRACE`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
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
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			var (
				resource *dockertest.Resource
				db       *sql.DB
			)

			resource, db, err = ResourceWithPostgreSQLVersion(t, pool, v)
			require.NoError(t, err)
			require.NoError(t, resource.Expire(120))

			hostAndPort := resource.GetHostPort("5432/tcp")
			hostAndPortSplited := strings.Split(hostAndPort, ":")
			password := "l]YLSc|4[i56%{gY"

			for i := 0; i < 1000; i++ {
				f := GetFakeFlightRecord()
				_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
				require.NoError(t, err)
			}

			databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
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
				msgBytes, err := mb[0].AsBytes()
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
				_ = streamOut.Run(t.Context())
			}()

			assert.Eventually(t, func() bool {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				return len(outBatches) == 1000
			}, time.Second*15, time.Millisecond*100)

			for i := 0; i < 1000; i++ {
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
			}, time.Second*15, time.Millisecond*100)

			require.NoError(t, streamOut.StopWithin(time.Second*10))

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
				assert.NoError(t, streamOut.Run(t.Context()))
			}()

			time.Sleep(time.Second * 5)
			for i := 0; i < 1000; i++ {
				f := GetFakeFlightRecord()
				_, err = db.Exec("INSERT INTO flights (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
				require.NoError(t, err)
			}

			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				assert.Len(c, outBatches, 1000, "got: %d", len(outBatches))
			}, time.Second*10, time.Millisecond*100)

			require.NoError(t, streamOut.StopWithin(time.Second*10))
		})
	}
}

func TestIntegrationTOASTValues(t *testing.T) {
	t.Parallel()
	integration.CheckSkip(t)

	for _, replicaIdentity := range []string{"FULL", "DEFAULT", "ALT_UNCHANGED_TOAST"} {
		replicaIdentity := replicaIdentity
		t.Run(replicaIdentity, func(t *testing.T) {
			t.Parallel()
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			var (
				resource *dockertest.Resource
				db       *sql.DB
			)

			resource, db, err = ResourceWithPostgreSQLVersion(t, pool, "16")
			require.NoError(t, err)
			require.NoError(t, resource.Expire(120))

			if replicaIdentity == "FULL" {
				_, err = db.Exec(`ALTER TABLE large_values REPLICA IDENTITY FULL`)
				require.NoError(t, err)
			}

			const stringSize = 400_000

			hostAndPort := resource.GetHostPort("5432/tcp")
			hostAndPortSplited := strings.Split(hostAndPort, ":")
			password := "l]YLSc|4[i56%{gY"

			require.NoError(t, err)

			// Insert a large >1MiB value
			_, err = db.Exec(`INSERT INTO large_values (id, value) VALUES ($1, $2);`, 1, strings.Repeat("foo", stringSize))
			require.NoError(t, err)

			databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
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
			require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: TRACE`))
			require.NoError(t, streamOutBuilder.AddInputYAML(template))

			var outBatches []string
			var outBatchMut sync.Mutex
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
				msgBytes, err := mb[0].AsBytes()
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
	t.Parallel()
	integration.CheckSkip(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		resource *dockertest.Resource
		db       *sql.DB
	)

	resource, db, err = ResourceWithPostgreSQLVersion(t, pool, "16")
	require.NoError(t, err)
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"

	require.NoError(t, err)

	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
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
	var count atomic.Int64
	writer := asyncroutine.NewPeriodic(time.Microsecond, func() {
		_, err := db.Exec("INSERT INTO seq DEFAULT VALUES")
		require.NoError(t, err)
		count.Add(1)
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
	expected := []int64{}
	for i := range count.Load() {
		expected = append(expected, i+1)
	}
	batchMu.Lock()
	require.Equal(t, expected, sequenceNumbers)
	batchMu.Unlock()
}

func TestIntegrationPostgresMetadata(t *testing.T) {
	t.Parallel()
	integration.CheckSkip(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		resource *dockertest.Resource
		db       *sql.DB
	)

	resource, db, err = ResourceWithPostgreSQLVersion(t, pool, "16")
	require.NoError(t, err)
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"

	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO "FlightsCompositePK" ("Seq", "Name", "CreatedAt") VALUES ($1, $2, $3);`, 1, "delta", "2006-01-02T15:04:05Z07:00")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO flights (name, created_at) VALUES ($1, $2);`, "delta", "2006-01-02T15:04:05Z07:00")
	require.NoError(t, err)

	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
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
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: TRACE`))
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
	t.Parallel()
	integration.CheckSkip(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		resource *dockertest.Resource
		db       *sql.DB
	)

	resource, db, err = ResourceWithPostgreSQLVersion(t, pool, "16")
	require.NoError(t, err)
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	password := "l]YLSc|4[i56%{gY"

	require.NoError(t, err)

	databaseURL := fmt.Sprintf("user=user_name password=%s dbname=dbname sslmode=disable host=%s port=%s", password, hostAndPortSplited[0], hostAndPortSplited[1])
	template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_native_decoder
    schema: public
    heartbeat_interval: 1s
    pg_standby_timeout: 1s
    tables:
      - flights
`, databaseURL)

	writer := asyncroutine.NewPeriodic(time.Millisecond, func() {
		_, err := db.Exec("INSERT INTO seq DEFAULT VALUES")
		require.NoError(t, err)
	})
	writer.Start()
	t.Cleanup(writer.Stop)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: TRACE`))
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
		require.NoError(t, streamOut.Run(t.Context()))
	}()
	time.Sleep(time.Second)

	getRestartLSN := func() string {
		for range 10 {
			rows, err := db.Query("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'test_slot_native_decoder'")
			require.NoError(t, err)
			for rows.Next() {
				var lsn string
				require.NoError(t, rows.Scan(&lsn))
				return lsn
			}
			require.NoError(t, rows.Err())
			time.Sleep(1 * time.Second)
		}
		require.FailNow(t, "unable to get replication slot position")
		return ""
	}

	// Make sure the LSN advances even when no messages are being emitted
	startLSN := getRestartLSN()
	require.Eventually(t, func() bool {
		return getRestartLSN() > startLSN
	}, 5*time.Second, 500*time.Millisecond)
	require.NoError(t, streamOut.StopWithin(time.Second*10))
}
