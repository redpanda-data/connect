package logminer

import (
	"database/sql"
	"fmt"
	"log"
)

// SessionManager manages LogMiner sessions
type SessionManager struct {
	db *sql.DB
}

func NewSessionManager(db *sql.DB) *SessionManager {
	return &SessionManager{db: db}
}

// AddLogFile registers a log file with LogMiner
//
//	func (sm *SessionManager) AddLogFile(fileName string) error {
//		sql := fmt.Sprintf("BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '%s', OPTIONS => DBMS_LOGMNR.ADDFILE); END;", fileName)
//		_, err := sm.db.Exec(sql)
//		if err != nil {
//			return fmt.Errorf("failed to add log file: %w", err)
//		}
//		log.Printf("Added log file: %s", fileName)
//		return nil
//	}

func (sm *SessionManager) AddLogFile(fileName string, isFirst bool) error {
	var option string
	if isFirst {
		option = "DBMS_LOGMNR.NEW" // Clears previous files and adds this one
	} else {
		option = "DBMS_LOGMNR.ADDFILE" // Adds to existing list
	}

	query := fmt.Sprintf("BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => %s); END;", option)
	_, err := sm.db.Exec(query, fileName)
	return err
}

func (sm *SessionManager) RemoveAllLogFiles() error {
	// No explicit removal needed - use NEW option on first AddLogFile instead
	return nil
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

	optionsStr := ""
	for i, opt := range options {
		if i > 0 {
			optionsStr += " + "
		}
		optionsStr += opt
	}

	sql := fmt.Sprintf(
		"BEGIN sys.dbms_logmnr.start_logmnr(startScn => %d, endScn => %d, options => %s); END;",
		startSCN, endSCN, optionsStr,
	)

	_, err := sm.db.Exec(sql)
	if err != nil {
		return fmt.Errorf("failed to start LogMiner session: %w", err)
	}

	log.Printf("Started LogMiner session: SCN %d to %d", startSCN, endSCN)
	return nil
}

// EndSession ends the current LogMiner session
func (sm *SessionManager) EndSession() error {
	_, err := sm.db.Exec("BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;")
	if err != nil {
		log.Printf("Warning: failed to end LogMiner session: %v", err)
	}
	return err
}
