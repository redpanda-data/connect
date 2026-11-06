// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// SessionManager manages LogMiner sessions, such as loading
// logs into LogMiner then starting/ending mining sessions.
type SessionManager struct {
	cfg           *Config
	opts          []string
	active        bool
	loadedFiles   []*LogFile
	sessionOpened time.Time
	log           *service.Logger

	lmStmtCache stmtCache
	endStmt     *sql.Stmt
}

// NewSessionManager creates a new SessionManager with the specified configuration.
// It initializes LogMiner options based on the mining strategy (e.g., DICT_FROM_ONLINE_CATALOG).
func NewSessionManager(cfg *Config, logger *service.Logger) *SessionManager {
	options := []string{
		"DBMS_LOGMNR.NO_ROWID_IN_STMT",
	}

	switch cfg.MiningStrategy {
	case OnlineCatalogStrategy:
		options = append(options, "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")
	default:
		options = append(options, "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")
	}

	return &SessionManager{
		cfg:         cfg,
		opts:        options,
		log:         logger,
		lmStmtCache: make(stmtCache),
	}
}

// logFilesChanged performance a filename check on whether newFiles differs from the currently loaded log files.
// If they're considered the same ADD_LOGFILE can be skipped.
func (sm *SessionManager) logFilesChanged(newFiles []*LogFile) bool {
	if len(sm.loadedFiles) != len(newFiles) {
		return true
	}
	for i, f := range sm.loadedFiles {
		if f.FileName != newFiles[i].FileName {
			return true
		}
	}
	return false
}

// AddLogFile adds one or more redo log files to the LogMiner session for mining, clearing
// previously loaded files before adding new files to the list of files to be mined.
func (sm *SessionManager) AddLogFile(ctx context.Context, conn *sql.Conn, files []*LogFile) error {
	for i, f := range files {
		opt := "DBMS_LOGMNR.ADDFILE"
		if i == 0 {
			opt = "DBMS_LOGMNR.NEW" // Clears previous files and adds this one
		}

		q := fmt.Sprintf("BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => %s); END;", opt)
		stmt, err := sm.lmStmtCache.getOrPrepare(ctx, conn, "add:"+opt, q)
		if err != nil {
			return fmt.Errorf("preparing logminer add log file statement with option '%s': %w", opt, err)
		}
		if _, err := stmt.ExecContext(ctx, f.FileName); err != nil {
			return fmt.Errorf("adding logminer log file '%s' with option '%s': %w", f.FileName, opt, err)
		}

		sm.log.Debugf("Loaded %s redo log file '%s' into LogMiner", f.Type, f.FileName)
	}

	sm.loadedFiles = files
	sm.sessionOpened = time.Now()
	return nil
}

// StartSession starts a LogMiner session with ONLINE_CATALOG strategy
func (sm *SessionManager) StartSession(ctx context.Context, conn *sql.Conn, startSCN, endSCN uint64, committedDataOnly bool) error {
	opts := make([]string, 0, len(sm.opts))
	opts = append(opts, sm.opts...)

	if committedDataOnly {
		opts = append(opts, []string{"DBMS_LOGMNR.COMMITTED_DATA_ONLY"}...)
	}

	optionsStr := strings.Join(opts, " + ")

	q := "BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => :1, ENDSCN => :2, OPTIONS => " + optionsStr + "); END;"
	stmt, err := sm.lmStmtCache.getOrPrepare(ctx, conn, "start:"+optionsStr, q)
	if err != nil {
		return fmt.Errorf("preparing start logminer session statement: %w", err)
	}
	if _, err := stmt.ExecContext(ctx, startSCN, endSCN); err != nil {
		return fmt.Errorf("starting logminer session: %w", err)
	}

	sm.active = true
	return nil
}

// EndSession ends the current LogMiner session
func (sm *SessionManager) EndSession(ctx context.Context, conn *sql.Conn) error {
	if sm.endStmt == nil {
		stmt, err := conn.PrepareContext(ctx, "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;")
		if err != nil {
			return fmt.Errorf("preparing end logminer session statement: %w", err)
		}
		sm.endStmt = stmt
	}
	if _, err := sm.endStmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("ending logminer session: %w", err)
	}

	sm.active = false
	sm.loadedFiles = nil
	sm.sessionOpened = time.Time{}
	return nil
}

// Close releases all statements prepared on the session's dedicated
// connection. It must only be called once the connection itself is done
// being used for LogMiner operations (i.e. at ReadChanges teardown), since a
// closed statement cannot be reused.
func (sm *SessionManager) Close() error {
	var errs []error

	errs = append(errs, sm.lmStmtCache.closeAll()...)
	sm.lmStmtCache = make(stmtCache)

	if sm.endStmt != nil {
		if err := sm.endStmt.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing end logminer statement: %w", err))
		}
		sm.endStmt = nil
	}

	return errors.Join(errs...)
}

// IsActive returns true if a LogMiner session is currently active.
func (sm *SessionManager) IsActive() bool {
	return sm.active
}

// Age returns how long the current LogMiner session has been open since its
// underlying log files were last (re)loaded via AddLogFile. Returns 0 if no
// session is active.
func (sm *SessionManager) Age() time.Duration {
	if sm.sessionOpened.IsZero() {
		return 0
	}
	return time.Since(sm.sessionOpened)
}

// IsExpired reports whether the current session has been open for at least
// maxAge. Always false when no session is active or maxAge is 0 (disabled).
func (sm *SessionManager) IsExpired(maxAge time.Duration) bool {
	return maxAge > 0 && sm.active && sm.Age() >= maxAge
}

// stmtCache is map keyed off of namespace + option to prevent accidental collisions.
type stmtCache map[string]*sql.Stmt

// getOrPrepare gets prepared statement from the cache or prepares and adds if does not exist.
func (c stmtCache) getOrPrepare(ctx context.Context, conn *sql.Conn, key, query string) (*sql.Stmt, error) {
	if stmt, exists := c[key]; exists {
		return stmt, nil
	}

	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	c[key] = stmt
	return stmt, nil
}

func (c stmtCache) closeAll() []error {
	var errs []error
	for key, stmt := range c {
		if err := stmt.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing prepared logminer statement '%s': %w", key, err))
		}
	}
	return errs
}
