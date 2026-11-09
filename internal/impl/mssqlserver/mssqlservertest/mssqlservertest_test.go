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

	t.Log("Given: a database with a leftover cdc schema, as a failed sp_cdc_enable_db can leave")
	ctr := sharedContainer(t)
	dbName := testDatabaseName(t)
	require.NoError(t, createDatabase(t.Context(), ctr, dbName))
	connStr, err := ctr.ConnectionString(t.Context(), "database="+dbName, "encrypt=disable")
	require.NoError(t, err)
	db, err := sql.Open("mssql", connStr)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE SCHEMA cdc;")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	t.Log("When: CDC is enabled on the database")
	_, _, err = openAndEnableCDC(t.Context(), ctr, dbName, t.Logf)

	t.Log("Then: the error reports the leftover")
	require.ErrorIs(t, err, errCDCLeftover)
}

func TestIntegrationLogDeadlocks(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: a database with two tables a and b")
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

	t.Log("When: two transactions lock the tables in opposite order and deadlock")
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
	require.NotEqual(t, hasSQLError(err1, errNumDeadlock), hasSQLError(err2, errNumDeadlock),
		"want one deadlock victim, got %v and %v", err1, err2)

	t.Log("Then: logDeadlocks logs the victim, the survivor, their statements, and the locked key")
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
