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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	_ "github.com/lib/pq"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS flights_composite_pks (
				id serial, seq integer, name VARCHAR(50), created_at TIMESTAMP,
				PRIMARY KEY (id, seq)
			);`)
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
	tmpDir := t.TempDir()
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
		_, err = db.Exec("INSERT INTO flights_composite_pks (seq, name, created_at) VALUES ($1, $2, $3);", i, f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
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
       - flights_composite_pks
`, databaseURL)

	cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
		_ = streamOut.Run(context.Background())
	}()

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10
	}, time.Second*25, time.Millisecond*100)

	for i := 10; i < 20; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights_composite_pks (seq, name, created_at) VALUES ($1, $2, $3);", i, f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO flights_non_streamed (name, created_at) VALUES ($1, $2);", f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
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
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	outBatches = []string{}
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
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
		assert.NoError(t, streamOut.Run(context.Background()))
	}()

	time.Sleep(time.Second * 5)
	for i := 20; i < 30; i++ {
		f := GetFakeFlightRecord()
		_, err = db.Exec("INSERT INTO flights_composite_pks (seq, name, created_at) VALUES ($1, $2, $3);", i, f.RealAddress.City, time.Unix(f.CreatedAt, 0).Format(time.RFC3339))
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
	tmpDir := t.TempDir()

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

	cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outMessages int64
	var outMessagesMut sync.Mutex

	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
		_ = streamOut.Run(context.Background())
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
	tmpDir := t.TempDir()
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

	cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
		err = streamOut.Run(context.Background())
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
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	outBatches = []string{}
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
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
		assert.NoError(t, streamOut.Run(context.Background()))
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
	tmpDir := t.TempDir()
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

	cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
		err = streamOut.Run(context.Background())
		require.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 1
	}, time.Second*25, time.Millisecond*100)

	messageWithComplexTypes := outBatches[0]

	// producing change to non-complex type to trigger replication and receive updated row so we can check the complex types again
	// but after they have been produced by replication to ensure the consistency
	_, err = db.Exec("UPDATE complex_types_example SET id = 2 WHERE id = 1")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 2
	}, time.Second*25, time.Millisecond*100)

	// replacing update with insert to remove replication messages type differences
	// so we will be checking only the data
	lastMessage := outBatches[len(outBatches)-1]
	lastMessage = strings.Replace(lastMessage, "update", "insert", 1)
	messageWithComplexTypes = strings.Replace(messageWithComplexTypes, "\"table_snapshot_progress\":0,", "", 1)

	require.Equal(t, messageWithComplexTypes, strings.Replace(lastMessage, ":2", ":1", 1))

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func TestIntegrationMultiplePostgresVersions(t *testing.T) {
	integration.CheckSkip(t)
	// running tests in the look to test different PostgreSQL versions
	for _, version := range []string{"17", "16", "15", "14", "13", "12", "11", "10"} {
		v := version
		t.Run(version, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
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
    schema: public
    tables:
       - flights
`, databaseURL)

			cacheConf := fmt.Sprintf(`
label: pg_stream_cache
file:
    directory: %v
`, tmpDir)

			streamOutBuilder := service.NewStreamBuilder()
			require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
			require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
			require.NoError(t, streamOutBuilder.AddInputYAML(template))

			var outBatches []string
			var outBatchMut sync.Mutex
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
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
				_ = streamOut.Run(context.Background())
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
			require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
			require.NoError(t, streamOutBuilder.AddInputYAML(template))

			outBatches = []string{}
			require.NoError(t, streamOutBuilder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
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
				assert.NoError(t, streamOut.Run(context.Background()))
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
