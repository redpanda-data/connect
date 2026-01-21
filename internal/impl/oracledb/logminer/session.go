package logminer

import (
	"database/sql"
	"fmt"
)

// SessionManager manages LogMiner sessions, such as loading
// logs into LogMiner then starting/ending mining sessions.
type SessionManager struct {
	db *sql.DB
}

func NewSessionManager(db *sql.DB) *SessionManager {
	return &SessionManager{db: db}
}

// AutoRegisterLogFile registers a log file with LogMiner
//	func (sm *SessionManager) AutoRegisterLogFile(fileName string) error {
//		sql := fmt.Sprintf("BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '%s', OPTIONS => DBMS_LOGMNR.ADDFILE); END;", fileName)
//		_, err := sm.db.Exec(sql)
//		if err != nil {
//			return fmt.Errorf("failed to add log file: %w", err)
//		}
//		log.Printf("Added log file: %s", fileName)
//		return nil
//	}

func (sm *SessionManager) AddLogFile(fileName string, isFirst bool) error {
	var opt string
	if isFirst {
		opt = "DBMS_LOGMNR.NEW" // Clears previous files and adds this one
	} else {
		opt = "DBMS_LOGMNR.ADDFILE" // Adds to existing list
	}

	q := fmt.Sprintf("BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => %s); END;", opt)
	_, err := sm.db.Exec(q, fileName)
	return err
}

// StartSession starts a LogMiner session with ONLINE_CATALOG strategy
func (sm *SessionManager) StartSession(startSCN, endSCN uint64, committedDataOnly bool) error {
	// Build options for ONLINE_CATALOG mode
	options := []string{
		"DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG", // Use current data dictionary
		"DBMS_LOGMNR.NO_ROWID_IN_STMT",         // Exclude ROWIDs from SQL
	}

	if committedDataOnly {
		options = append(options, "DBMS_LOGMNR.COMMITTED_DATA_ONLY")
	}

	var optionsStr string
	for i, o := range options {
		if i > 0 {
			optionsStr += " + "
		}
		optionsStr += o
	}

	q := fmt.Sprintf("BEGIN sys.dbms_logmnr.start_logmnr(startScn => %d, endScn => %d, options => %s); END;", startSCN, endSCN, optionsStr)
	if _, err := sm.db.Exec(q); err != nil {
		return fmt.Errorf("starting LogMiner session: %w", err)
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
