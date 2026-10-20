// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cdc

import (
	"encoding/json"
	"fmt"
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/connect/v4/internal/replication"
)

// decodeMongoSignal reads a document inserted into the signal collection:
// {"type": <string>, "data": <document or JSON string>}. It returns the
// decoded signal and its data as JSON, for payloads DecodeSignal leaves to
// the caller.
//
// A malformed document wraps replication.ErrSignalRejected: it can never be
// honoured, so the caller logs it and carries on.
func decodeMongoSignal(doc bson.Raw) (*replication.ControlSignal, []byte, error) {
	signalType, ok := doc.Lookup("type").StringValueOK()
	if !ok {
		return nil, nil, fmt.Errorf("%w: signal document has no string type field", replication.ErrSignalRejected)
	}

	var data []byte
	switch raw := doc.Lookup("data"); raw.Type {
	case bson.Type(0), bson.TypeNull:
	case bson.TypeString:
		data = []byte(raw.StringValue())
	case bson.TypeEmbeddedDocument:
		var err error
		if data, err = bson.MarshalExtJSON(raw.Document(), false, false); err != nil {
			return nil, nil, fmt.Errorf("%w: encoding signal data: %w", replication.ErrSignalRejected, err)
		}
	default:
		return nil, nil, fmt.Errorf("%w: signal data must be a document or a JSON string, got %s", replication.ErrSignalRejected, raw.Type)
	}

	sig, err := replication.DecodeSignal(signalID(doc.Lookup("_id")), signalType, data)
	if err != nil {
		return nil, nil, err
	}
	return sig, data, nil
}

// signalID renders a signal's _id for logs: the text of a string or an
// ObjectId, otherwise its relaxed Extended JSON.
func signalID(id bson.RawValue) string {
	if s, ok := id.StringValueOK(); ok {
		return s
	}
	if oid, ok := id.ObjectIDOK(); ok {
		return oid.Hex()
	}
	if b, err := bson.MarshalExtJSON(bson.D{{Key: "v", Value: id}}, false, false); err == nil {
		var wrapper struct {
			V json.RawMessage `json:"v"`
		}
		if json.Unmarshal(b, &wrapper) == nil {
			return string(wrapper.V)
		}
	}
	return id.String()
}

// parseSnapshotCollections reads a snapshot-execute payload. MongoDB has
// collections rather than tables, so "collections" is accepted alongside the
// shared "tables"; the two are merged.
func parseSnapshotCollections(data []byte) ([]string, error) {
	var payload struct {
		Collections []string `json:"collections"`
		replication.SnapshotSignal
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("%w: parsing %s payload: %w", replication.ErrSignalRejected, replication.SnapshotSignalType, err)
	}
	var merged []string
	for _, c := range slices.Concat(payload.Collections, payload.Tables) {
		if !slices.Contains(merged, c) {
			merged = append(merged, c)
		}
	}
	if len(merged) == 0 {
		return nil, fmt.Errorf("%w: %s payload lists no collections", replication.ErrSignalRejected, replication.SnapshotSignalType)
	}
	return merged, nil
}

// handleSignal acts on a change event inserting into the signal collection.
// run is the connection's incremental snapshot, nil when there is none.
//
// Nothing here fails the stream: a signal that cannot be honoured is logged and
// skipped, since redelivering it would fail the same way.
func (m *mongoCDC) handleSignal(event bson.Raw, run *incSnapshotRun) {
	doc, ok := event.Lookup("fullDocument").DocumentOK()
	if !ok {
		m.logger.Errorf("Signal insert in %s carried no document, skipping it", m.signalCollection)
		return
	}
	sig, data, err := decodeMongoSignal(doc)
	if err != nil {
		m.logger.Errorf("Skipping signal in %s: %v", m.signalCollection, err)
		return
	}
	log := m.logger.With("id", sig.ID, "type", sig.SignalType)

	switch sig.SignalType {
	case replication.LogSignalType:
		log.Infof("%s", sig.Message)
	case replication.SnapshotSignalType:
		if run == nil {
			log.Warnf("%v: set %s: %s", replication.ErrSnapshotDisabled, fieldSnapshotMode, snapshotModeIncremental)
			return
		}
		collections, err := parseSnapshotCollections(data)
		if err != nil {
			log.Errorf("Skipping signal: %v", err)
			return
		}
		var unknown []string
		for _, c := range collections {
			if !slices.Contains(m.collections, c) {
				unknown = append(unknown, c)
			}
		}
		if len(unknown) > 0 {
			// The stream does not watch them, so nothing could deduplicate the
			// chunks against later changes.
			log.Errorf("Skipping signal: %v: collections %v are not in %s", replication.ErrSignalRejected, unknown, fieldCollections)
			return
		}
		added := run.request(collections)
		if skipped := len(slices.Compact(slices.Sorted(slices.Values(collections)))) - len(added); skipped > 0 {
			log.Infof("%d of the requested collections are already queued or being read, not queueing them again", skipped)
		}
		if len(added) > 0 {
			log.Infof("Incremental snapshot queued collections %v", added)
		}
	default:
		log.Warnf("Signal received but not a recognized type, skipping it")
	}
}
