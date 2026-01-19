package logminer

import (
	"fmt"
	"regexp"
	"strings"
)

// EventProcessor handles event parsing and conversion
type EventProcessor struct{}

func NewEventProcessor() *EventProcessor {
	return &EventProcessor{}
}

// parseRedoUndoValue parses the REDO_VALUE or UNDO_VALUE field into a map
// Format is typically: "COL1 = 'value1' COL2 = 123 COL3 = 'value3'"
func parseRedoUndoValue(value string) (map[string]any, error) {
	if value == "" {
		return nil, nil
	}

	result := make(map[string]any)

	// Pattern to match: COLUMN_NAME = 'value' or COLUMN_NAME = numeric
	// This regex handles quoted strings and numeric values
	pattern := regexp.MustCompile(`(\w+)\s*=\s*(?:'([^']*)'|([^'\s]+))`)
	matches := pattern.FindAllStringSubmatch(value, -1)

	for _, match := range matches {
		if len(match) >= 3 {
			colName := match[1]
			// match[2] is quoted value, match[3] is unquoted value
			if match[2] != "" {
				result[colName] = match[2] // quoted string
			} else if match[3] != "" {
				result[colName] = match[3] // numeric or unquoted value
			}
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("failed to parse redo/undo value: %s", value)
	}

	return result, nil
}

// ParseDML parses a LogMiner event into a DML event
func (ep *EventProcessor) ParseDML(event *LogMinerEvent) (*DMLEvent, error) {
	dml := &DMLEvent{
		Operation: event.Operation,
		// SQLRedo:   event.RedoValue.String,
		Timestamp: event.Timestamp,
	}

	if event.SchemaName.Valid {
		dml.Schema = event.SchemaName.String
	}
	if event.TableName.Valid {
		dml.Table = event.TableName.String
	}

	// Store SQL_REDO - will need to parse this to extract column values
	if strings.TrimSpace(event.SQLRedo) != "" {
		dml.SQLRedo = event.SQLRedo
	}

	// if event.RedoValue.Valid && strings.TrimSpace(event.RedoValue.String) != "" {
	// 	newValues, err := parseRedoUndoValue(event.RedoValue.String)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("parsing redo value: %w", err)
	// 	}
	// 	dml.NewValues = newValues
	// }

	// // Parse UNDO_VALUE for old values (UPDATE/DELETE)
	// if event.UndoValue.Valid && strings.TrimSpace(event.UndoValue.String) != "" {
	// 	oldValues, err := parseRedoUndoValue(event.UndoValue.String)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("parsing undo value: %w", err)
	// 	}
	// 	dml.OldValues = oldValues
	// }

	return dml, nil
}

// ConvertToChangeEvent converts a DML event to a change event
func (ep *EventProcessor) ConvertToChangeEvent(dml *DMLEvent, scn int64) *ChangeEvent {
	ce := &ChangeEvent{
		Schema:    dml.Schema,
		Table:     dml.Table,
		SCN:       scn,
		Timestamp: dml.Timestamp,
		Data:      dml.SQLRedo,
	}

	switch dml.Operation {
	case OpInsert:
		ce.Operation = "CREATE"
	case OpUpdate:
		ce.Operation = "UPDATE"
	case OpDelete:
		ce.Operation = "DELETE"
	}

	return ce
}
