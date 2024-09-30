// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pg_stream_schemaless

import (
	"bytes"
	"context"

	"encoding/json"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	configSpec := service.NewConfigSpec()

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newPgSchematicProcessor(mgr.Logger(), mgr.Metrics()), nil
	}

	err := service.RegisterProcessor("pg_stream_schemaless", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

type pgSchematicProcessor struct {
}

func newPgSchematicProcessor(logger *service.Logger, metrics *service.Metrics) *pgSchematicProcessor {
	// The logger and metrics components will already be labelled with the
	// identifier of this component within a config.
	return &pgSchematicProcessor{}
}

func (r *pgSchematicProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	bytesContent, err := m.AsBytes()
	if err != nil {
		return nil, err
	}
	var message WalMessage
	if err = json.NewDecoder(bytes.NewReader(bytesContent)).Decode(&message); err != nil {
		return nil, err
	}

	var messageAsSchema = map[string]interface{}{}
	if len(message.Change) == 0 {
		return nil, nil
	}

	for _, change := range message.Change {
		for i, k := range change.Columnnames {
			messageAsSchema[k] = change.Columnvalues[i]
		}
	}
	var newBytes []byte
	if newBytes, err = json.Marshal(&messageAsSchema); err != nil {
		return nil, err
	}

	m.SetBytes(newBytes)
	return []*service.Message{m}, nil
}

func (r *pgSchematicProcessor) Close(ctx context.Context) error {
	return nil
}
