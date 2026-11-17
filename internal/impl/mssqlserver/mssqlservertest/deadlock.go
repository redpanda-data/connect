// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlservertest

import (
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"fmt"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	tcmssql "github.com/testcontainers/testcontainers-go/modules/mssql"
)

// errNumDeadlock is the SQL Server error of the deadlock victim. Refer to
// https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/mssqlserver-1205-database-engine-error
const errNumDeadlock = 1205

// deadlockSessionName is the Extended Events session that startDeadlockSession creates. It records each deadlock
// graph in a ring buffer, so that logDeadlocks can show the other side of a 1205 error.
//
// The built-in system_health session also records deadlock graphs, but it writes them up to 120s late
// (MAX_DISPATCH_LATENCY), which is after the test fails.
const deadlockSessionName = "rpcn_deadlocks"

// startDeadlockSession creates and starts deadlockSessionName on the server of ctr.
func startDeadlockSession(ctx context.Context, ctr *tcmssql.MSSQLServerContainer) error {
	connStr, err := ctr.ConnectionString(ctx, "encrypt=disable")
	if err != nil {
		return err
	}
	db, err := sql.Open("mssql", connStr)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `
		IF NOT EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = N'`+deadlockSessionName+`')
			CREATE EVENT SESSION [`+deadlockSessionName+`] ON SERVER
			ADD EVENT sqlserver.xml_deadlock_report
			ADD TARGET package0.ring_buffer
			WITH (MAX_DISPATCH_LATENCY = 1 SECONDS);
		IF NOT EXISTS (SELECT 1 FROM sys.dm_xe_sessions WHERE name = N'`+deadlockSessionName+`')
			ALTER EVENT SESSION [`+deadlockSessionName+`] ON SERVER STATE = START;`)
	return err
}

// hasSQLError reports whether err has SQL Server error number num, also as an earlier error of the same batch.
func hasSQLError(err error, num int32) bool {
	var msErr mssql.Error
	if !errors.As(err, &msErr) {
		return false
	}
	if msErr.Number == num {
		return true
	}
	for _, e := range msErr.All {
		if e.Number == num {
			return true
		}
	}
	return false
}

// deadlockEvent is the part of an xml_deadlock_report event that logDeadlocks shows.
type deadlockEvent struct {
	Timestamp string `xml:"timestamp,attr"`
	Victims   []struct {
		ID string `xml:"id,attr"`
	} `xml:"data>value>deadlock>victim-list>victimProcess"`
	Processes []struct {
		ID       string `xml:"id,attr"`
		Database string `xml:"currentdbname,attr"`
		LockMode string `xml:"lockMode,attr"`
		Frames   []struct {
			ProcName string `xml:"procname,attr"`
		} `xml:"executionStack>frame"`
		InputBuf string `xml:"inputbuf"`
	} `xml:"data>value>deadlock>process-list>process"`
	ResourceList struct {
		Resources []struct {
			XMLName    xml.Name
			ObjectName string `xml:"objectname,attr"`
			Mode       string `xml:"mode,attr"`
		} `xml:",any"`
	} `xml:"data>value>deadlock>resource-list"`
}

// logDeadlocks logs each deadlock graph of the last 5 minutes that deadlockSessionName recorded: for each process
// the database, the procedure frames and the input, and the locked resources. It waits up to 3s for the graph of a
// deadlock that just happened, because the session sends events to the ring buffer up to 1s late.
func logDeadlocks(ctx context.Context, db *sql.DB, logf func(format string, args ...any)) {
	var events []deadlockEvent
	for range 3 {
		var data sql.NullString
		err := db.QueryRowContext(ctx, `
			SELECT CAST(t.target_data AS NVARCHAR(MAX))
			FROM sys.dm_xe_session_targets t
			JOIN sys.dm_xe_sessions s ON s.address = t.event_session_address
			WHERE s.name = N'`+deadlockSessionName+`' AND t.target_name = N'ring_buffer'`).Scan(&data)
		if err != nil {
			logf("read deadlock graphs: %v", err)
			return
		}
		var buf struct {
			Events []deadlockEvent `xml:"event"`
		}
		if err := xml.Unmarshal([]byte(data.String), &buf); err != nil {
			logf("parse deadlock graphs: %v", err)
			return
		}
		events = events[:0]
		for _, e := range buf.Events {
			if ts, err := time.Parse(time.RFC3339Nano, e.Timestamp); err == nil && time.Since(ts) < 5*time.Minute {
				events = append(events, e)
			}
		}
		if len(events) > 0 {
			break
		}
		time.Sleep(time.Second)
	}
	if len(events) == 0 {
		logf("no deadlock graph in %s", deadlockSessionName)
		return
	}
	for i, e := range events {
		victims := map[string]bool{}
		for _, v := range e.Victims {
			victims[v.ID] = true
		}
		var b strings.Builder
		fmt.Fprintf(&b, "deadlock %d/%d at %s:", i+1, len(events), e.Timestamp)
		for _, p := range e.Processes {
			procs := make([]string, 0, len(p.Frames))
			for _, f := range p.Frames {
				if f.ProcName != "" && f.ProcName != "adhoc" && f.ProcName != "unknown" {
					procs = append(procs, f.ProcName)
				}
			}
			fmt.Fprintf(&b, "\n  process %s victim=%v db=%s lock=%s frames=%v input=%q",
				p.ID, victims[p.ID], p.Database, p.LockMode, procs, strings.Join(strings.Fields(p.InputBuf), " "))
		}
		for _, r := range e.ResourceList.Resources {
			fmt.Fprintf(&b, "\n  resource %s object=%s mode=%s", r.XMLName.Local, r.ObjectName, r.Mode)
		}
		logf("%s", b.String())
	}
}
