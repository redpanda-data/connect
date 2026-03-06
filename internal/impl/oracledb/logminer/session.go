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
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// SessionManager manages LogMiner sessions, such as loading
// logs into LogMiner then starting/ending mining sessions.
type SessionManager struct {
	cfg    *Config
	opts   []string
	active bool
	log    *service.Logger
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
		cfg:  cfg,
		opts: options,
		log:  logger,
	}
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
		if _, err := conn.ExecContext(ctx, q, f.FileName); err != nil {
			return fmt.Errorf("adding logminer log file '%s' with option '%s': %w", f.FileName, opt, err)
		}

		sm.log.Debugf("Loaded redo log file '%s' into LogMiner", f.FileName)
	}

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

	q := fmt.Sprintf("BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => %d, ENDSCN => %d, OPTIONS => %s); END;", startSCN, endSCN, optionsStr)
	if _, err := conn.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("starting logminer session: %w", err)
	}

	sm.active = true
	return nil
}

// EndSession ends the current LogMiner session
func (sm *SessionManager) EndSession(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;"); err != nil {
		return fmt.Errorf("ending logminer session: %w", err)
	}

	sm.active = false
	return nil
}

// IsActive returns true if a LogMiner session is currently active.
func (sm *SessionManager) IsActive() bool {
	return sm.active
}
