package logminer

import (
	"strings"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// EventProcessor handles event parsing and conversion
type EventProcessor struct{}

func NewEventProcessor() *EventProcessor {
	return &EventProcessor{}
}

// ParseDML parses a LogMiner event into a DML event
// TODO: Can we do without this and instead do it as part of translating to replication.MessageEvent?
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
	if strings.TrimSpace(event.SQLRedo.String) != "" {
		dml.SQLRedo = event.SQLRedo.String
	}
	dml.Data = event.Data

	return dml, nil
}

// ConvertToChangeEvent converts a DML event to a change event
func (ep *EventProcessor) ConvertToChangeEvent(dml *DMLEvent, scn int64) *ChangeEvent {
	ce := &ChangeEvent{
		Schema:    dml.Schema,
		Table:     dml.Table,
		SCN:       replication.SCN(scn),
		Timestamp: dml.Timestamp,
		Data:      dml.Data,
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

func (ep *EventProcessor) toEventMessage(dml *DMLEvent, scn int64) *replication.MessageEvent {
	m := &replication.MessageEvent{
		SCN:       replication.SCN(scn),
		Schema:    dml.Schema,
		Table:     dml.Table,
		Data:      dml.Data,
		Timestamp: dml.Timestamp,
	}

	switch dml.Operation {
	case OpInsert:
		m.Operation = "CREATE"
	case OpUpdate:
		m.Operation = "UPDATE"
	case OpDelete:
		m.Operation = "DELETE"
	}

	return m
}
