package peerdb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/lib/pq"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/benthosdev/benthos/v4/public/service/integration"
)

func TestPeerDBIntegration(t *testing.T) {
	// Skip test if integration tests are disabled
	integration.CheckSkip(t)
	t.Parallel()

	// Create a new docker pool
	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Could not connect to docker")
	pool.MaxWait = 3 * time.Minute

	// Run a postgres container
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "debezium/postgres",
		Tag:        "14-alpine",
		Env: []string{
			"POSTGRES_USER=testuser",
			"POSTGRES_PASSWORD=testpass",
			"POSTGRES_DB=testdb",
		},
		ExposedPorts: []string{"5432"},
	})
	require.NoError(t, err, "Could not start postgres container")

	// Cleanup container after test
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource), "Failed to cleanup postgres container")
	})

	// Build postgres connection string
	port := resource.GetPort("5432/tcp")
	postgresURL := fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", port)

	// Wait for postgres to be ready
	assert.NoError(t, pool.Retry(func() error {
		db, err := sql.Open("postgres", postgresURL)
		if err != nil {
			return err
		}
		defer db.Close()
		return db.Ping()
	}), "Timed out waiting for postgres to be ready")
	// Create test table
	db, err := sql.Open("postgres", postgresURL)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE footable (
		id SERIAL PRIMARY KEY,
		foo TEXT,
		bar INTEGER,
		baz TEXT
	)`)
	require.NoError(t, err)

	// create a replication slot called test_replication_slot
	_, err = db.Exec(`SELECT * FROM pg_create_logical_replication_slot('test_replication_slot', 'pgoutput')`)
	require.NoError(t, err)

	// create a publication called test_publication for all tables
	_, err = db.Exec(`CREATE PUBLICATION test_publication FOR ALL TABLES`)
	require.NoError(t, err)

	uuid := uuid.New()
	cacheKey := fmt.Sprintf("peerdb_state_%s", uuid)

	conf := fmt.Sprintf(`
    host: localhost
    port: %s
    user: testuser
    password: testpass
    database: testdb
    tables:
      - public.footable
    replication_slot: test_replication_slot
    publication: test_publication
    cache: test_cache
    cache_key: %s
    auto_replay_nacks: false
`, port, cacheKey)

	inputConfig := peerDBInputConfig()
	env := service.NewEnvironment()

	// Create input from config
	parsedInput, err := inputConfig.ParseYAML(conf, env)
	require.NoError(t, err)

	res := service.MockResources(service.MockResourcesOptAddCache("test_cache"))
	input, err := newPeerDBInput(parsedInput, res)
	require.NoError(t, err)

	// Cleanup input after test
	t.Cleanup(func() {
		input.Close(context.Background())
	})

	err = input.Connect(context.Background())
	require.NoError(t, err)

	// write 2 rows to the table, twice
	_, err = db.Exec(`INSERT INTO footable (foo, bar, baz) VALUES ('foo1', 1, 'baz1'), ('foo2', 2, 'baz2')`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO footable (foo, bar, baz) VALUES ('foo3', 3, 'baz3'), ('foo4', 4, 'baz4')`)
	require.NoError(t, err)

	// Read messages from input
	readCtx, readDone := context.WithTimeout(context.Background(), time.Second*10)
	defer readDone()

	readMessages := make([]*service.Message, 0)
	for {
		batch, ackFn, err := input.ReadBatch(readCtx)
		if err != nil {
			fmt.Println("Error reading message: ", err)
			break
		}

		for _, msg := range batch {
			if msg == nil {
				continue
			}
			readMessages = append(readMessages, msg)
		}

		err = ackFn(readCtx, nil)
		require.NoError(t, err)
	}

	// Assert read messages match sent messages
	require.Len(t, readMessages, 4)
	for i, msg := range readMessages {
		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)
		assert.JSONEq(t, fmt.Sprintf(`{"bar":"%d","baz":"baz%d","foo":"foo%d","id":"%d"}`, i+1, i+1, i+1, i+1), string(msgBytes))
	}
}
