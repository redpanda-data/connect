// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Parser parses SQL_REDO statements from Oracle LogMiner.
// It handles the fixed forms that LogMiner produces:
//
//	INSERT: insert into "schema"."table"("C1","C2") values ('v1','v2');
//	UPDATE: update "schema"."table" set "C1" = 'v1', "C2" = 'v2' where "C1" = 'old1' and "C2" = 'old2';
//	DELETE: delete from "schema"."table" where "C1" = 'v1' and "C2" = 'v2';
type Parser struct {
	valueConverter OracleValueConverter
}

// NewParser creates a new Parser instance for parsing SQL_REDO statements.
func NewParser() *Parser {
	return &Parser{
		valueConverter: NewOracleValueConverter(time.UTC),
	}
}

// RedoEventToDMLEvent converts a RedoEvent (from V$LOGMNR_CONTENTS) into a DMLEvent.
func (p Parser) RedoEventToDMLEvent(redoEvent *RedoEvent) (DMLEvent, error) {
	if len(redoEvent.SQLRedo.String) == 0 {
		return DMLEvent{}, errors.New("empty SQL statement")
	}

	event := DMLEvent{
		Operation:     redoEvent.Operation,
		Timestamp:     redoEvent.Timestamp,
		TransactionID: redoEvent.TransactionID,
	}

	if redoEvent.SchemaName.Valid {
		event.Schema = redoEvent.SchemaName.String
	}
	if redoEvent.TableName.Valid {
		event.Table = redoEvent.TableName.String
	}

	if strings.TrimSpace(redoEvent.SQLRedo.String) != "" {
		event.SQLRedo = redoEvent.SQLRedo.String
	}

	newValues, oldValues, err := ScanSQLCommand(redoEvent.SQLRedo.String, &p.valueConverter)
	if err != nil {
		return DMLEvent{}, fmt.Errorf("parsing sql from redo log: %w", err)
	}

	event.Data = newValues
	event.OldValues = oldValues

	return event, nil
}
