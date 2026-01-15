package logminer

// EventProcessor handles event parsing and conversion
type EventProcessor struct{}

func NewEventProcessor() *EventProcessor {
	return &EventProcessor{}
}

// ParseDML parses a LogMiner event into a DML event
func (ep *EventProcessor) ParseDML(event *LogMinerEvent) (*DMLEvent, error) {
	dml := &DMLEvent{
		Operation: event.Operation,
		SQLRedo:   event.SQLRedo,
		Timestamp: event.Timestamp,
	}

	if event.SchemaName.Valid {
		dml.Schema = event.SchemaName.String
	}
	if event.TableName.Valid {
		dml.Table = event.TableName.String
	}

	// In real implementation, would parse SQL_REDO to extract column values
	// For now, just store the SQL statement
	dml.NewValues = map[string]any{
		"_sql": event.SQLRedo,
	}

	return dml, nil
}

// ConvertToChangeEvent converts a DML event to a change event
func (ep *EventProcessor) ConvertToChangeEvent(dml *DMLEvent, scn int64) *ChangeEvent {
	ce := &ChangeEvent{
		Schema:    dml.Schema,
		Table:     dml.Table,
		SCN:       scn,
		Timestamp: dml.Timestamp,
	}

	switch dml.Operation {
	case OpInsert:
		ce.Operation = "CREATE"
		ce.After = dml.NewValues
	case OpUpdate:
		ce.Operation = "UPDATE"
		ce.Before = dml.OldValues
		ce.After = dml.NewValues
	case OpDelete:
		ce.Operation = "DELETE"
		ce.Before = dml.OldValues
	}

	return ce
}
