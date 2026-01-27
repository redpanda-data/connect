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
func (EventProcessor) ParseDML(event *LMEvent) (*DMLEvent, error) {
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

// toEventMessage converts a DML event to a replication.toEventMessage ready for pushing to Benthos.
func (EventProcessor) toEventMessage(dml *DMLEvent, scn int64) *replication.MessageEvent {
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
