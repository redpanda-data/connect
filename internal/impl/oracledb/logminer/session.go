package logminer

import (
	"database/sql"
	"fmt"
)

// SessionManager manages LogMiner sessions, such as loading
// logs into LogMiner then starting/ending mining sessions.
type SessionManager struct {
	db   *sql.DB
	cfg  *Config
	opts []string
}

// NewSessionManager creates a new SessionManager with the specified database connection and configuration.
// It initializes LogMiner options based on the mining strategy (e.g., DICT_FROM_ONLINE_CATALOG).
func NewSessionManager(db *sql.DB, cfg *Config) *SessionManager {
	options := []string{
		"DBMS_LOGMNR.NO_ROWID_IN_STMT", // Exclude ROWIDs from SQL
	}

	switch cfg.MiningStrategy {
	case OnlineCatalogStrategy:
		options = append(options, "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")
	default:
		options = append(options, "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")
	}

	return &SessionManager{
		db:   db,
		cfg:  cfg,
		opts: options,
	}
}

// AddLogFile adds a redo log file to the LogMiner session for mining.
// If isFirst is true, it clears any previously added files before adding this one.
// Otherwise, it adds the file to the existing list of files to be mined.
func (sm *SessionManager) AddLogFile(filename string, isFirst bool) error {
	var opt string
	if isFirst {
		opt = "DBMS_LOGMNR.NEW" // Clears previous files and adds this one
	} else {
		opt = "DBMS_LOGMNR.ADDFILE" // Adds to existing list
	}

	q := fmt.Sprintf("BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => %s); END;", opt)
	if _, err := sm.db.Exec(q, filename); err != nil {
		return fmt.Errorf("adding logminer log file '%s' with option '%s': %w", filename, opt, err)
	}

	return nil
}

// StartSession starts a LogMiner session with ONLINE_CATALOG strategy
func (sm *SessionManager) StartSession(startSCN, endSCN uint64, committedDataOnly bool) error {
	// TODO: Ugh, optimise this
	opts := make([]string, len(sm.opts))
	opts = append(opts, sm.opts...)

	if committedDataOnly {
		opts = append(opts, []string{"DBMS_LOGMNR.COMMITTED_DATA_ONLY"}...)
	}

	var optionsStr string
	for i, o := range opts {
		if i > 0 {
			optionsStr += " + "
		}
		optionsStr += o
	}

	q := fmt.Sprintf("BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => %d, ENDSCN => %d, OPTIONS => %s); END;", startSCN, endSCN, optionsStr)
	if _, err := sm.db.Exec(q); err != nil {
		return fmt.Errorf("starting logminer session: %w", err)
	}

	return nil
}

// EndSession ends the current LogMiner session
func (sm *SessionManager) EndSession() error {
	if _, err := sm.db.Exec("BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;"); err != nil {
		return fmt.Errorf("ending logminer session: %w", err)
	}

	return nil
}
