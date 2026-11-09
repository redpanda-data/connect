// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlservertest

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestMain(m *testing.M) {
	code := m.Run()
	TerminateSharedContainer()
	os.Exit(code)
}

func TestIntegrationEnableCDCWithLeftoverSchema(t *testing.T) {
	integration.CheckSkip(t)

	ctr := sharedContainer(t)
	dbName := testDatabaseName(t)
	require.NoError(t, createDatabase(t.Context(), ctr, dbName))

	// A failed sp_cdc_enable_db can leave the cdc schema in the database.
	connStr, err := ctr.ConnectionString(t.Context(), "database="+dbName, "encrypt=disable")
	require.NoError(t, err)
	db, err := sql.Open("mssql", connStr)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE SCHEMA cdc;")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, _, err = openAndEnableCDC(t.Context(), ctr, dbName, t.Logf)
	require.ErrorIs(t, err, errCDCLeftover)
}

func TestIntegrationSetupRecreatesDatabaseWithLeftoverSchema(t *testing.T) {
	integration.CheckSkip(t)

	ctr := sharedContainer(t)
	dbName := testDatabaseName(t)

	// Leave the cdc schema after the first create, as a failed sp_cdc_enable_db does.
	creates := 0
	afterCreate := func() {
		creates++
		if creates > 1 {
			return
		}
		connStr, err := ctr.ConnectionString(t.Context(), "database="+dbName, "encrypt=disable")
		require.NoError(t, err)
		db, err := sql.Open("mssql", connStr)
		require.NoError(t, err)
		defer db.Close()
		_, err = db.ExecContext(t.Context(), "CREATE SCHEMA cdc;")
		require.NoError(t, err)
	}

	db, _, err := createDatabaseWithCDC(t.Context(), ctr, dbName, t.Logf, afterCreate)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	require.Equal(t, 2, creates)

	var enabled bool
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()").Scan(&enabled))
	require.True(t, enabled)
}

func TestIntegrationLogDeadlocks(t *testing.T) {
	integration.CheckSkip(t)

	ctr := sharedContainer(t)
	dbName := testDatabaseName(t)
	require.NoError(t, createDatabase(t.Context(), ctr, dbName))
	connStr, err := ctr.ConnectionString(t.Context(), "database="+dbName, "encrypt=disable")
	require.NoError(t, err)
	db, err := sql.Open("mssql", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	_, err = db.ExecContext(t.Context(), `
		CREATE TABLE a (id INT PRIMARY KEY); INSERT INTO a VALUES (1);
		CREATE TABLE b (id INT PRIMARY KEY); INSERT INTO b VALUES (1);`)
	require.NoError(t, err)

	// Each transaction locks one table, waits for the other to do the same, and then locks the other table.
	var locked sync.WaitGroup
	locked.Add(2)
	errs := make(chan error, 2)
	for _, order := range [][2]string{{"a", "b"}, {"b", "a"}} {
		go func() {
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				locked.Done()
				errs <- err
				return
			}
			defer func() { _ = tx.Rollback() }()
			_, err = tx.ExecContext(t.Context(), fmt.Sprintf("UPDATE %s SET id = 1", order[0]))
			locked.Done()
			if err != nil {
				errs <- err
				return
			}
			locked.Wait()
			_, err = tx.ExecContext(t.Context(), fmt.Sprintf("UPDATE %s SET id = 1", order[1]))
			errs <- err
		}()
	}
	err1, err2 := <-errs, <-errs
	require.True(t, hasSQLError(err1, errNumDeadlock) != hasSQLError(err2, errNumDeadlock),
		"want one deadlock victim, got %v and %v", err1, err2)

	var lines []string
	logDeadlocks(t.Context(), db, func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	})
	out := strings.Join(lines, "\n")
	t.Log(out)
	require.Contains(t, out, "victim=true")
	require.Contains(t, out, "victim=false")
	require.Contains(t, out, `input="UPDATE a SET id = 1"`)
	require.Contains(t, out, `input="UPDATE b SET id = 1"`)
	require.Contains(t, out, "resource keylock object="+dbName+".dbo.a")
}
