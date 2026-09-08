// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// ConnectorMapper maps a connector.class to an RPCN input and/or output.
type ConnectorMapper interface {
	Map(cfg ConnectConfig, ctx *MapCtx) (Component, error)
}

// ConverterMapper maps a key/value.converter to deserialization processors.
type ConverterMapper interface {
	Map(role ConverterRole, ctx *MapCtx) ([]*yaml.Node, error)
}

// SMTMapper maps a single SMT to ordered processor nodes.
type SMTMapper interface {
	Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error)
}

var (
	connectorMappers = map[string]ConnectorMapper{}
	converterMappers = map[string]ConverterMapper{}
	smtMappers       = map[string]SMTMapper{}
)

func registerConnector(class string, m ConnectorMapper) { connectorMappers[class] = m }
func registerConverter(class string, m ConverterMapper) { converterMappers[class] = m }
func registerSMT(typ string, m SMTMapper)               { smtMappers[typ] = m }

func lookupConnector(class string) ConnectorMapper {
	if m, ok := connectorMappers[class]; ok {
		return m
	}
	return fallbackConnector{}
}

func lookupConverter(class string) (ConverterMapper, bool) {
	m, ok := converterMappers[class]
	return m, ok
}

func lookupSMT(typ string) (SMTMapper, bool) {
	m, ok := smtMappers[typ]
	return m, ok
}

// fallbackConnector emits a commented stub for unknown connector classes so
// conversion never hard-fails.
type fallbackConnector struct{}

func (fallbackConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	ctx.Warn("connector.class", fmt.Sprintf("unsupported connector class %q", cfg.Class))
	body := mapping()
	body.LineComment = fmt.Sprintf("TODO: unsupported connector.class=%s — map manually", cfg.Class)
	return Component{Output: component("drop", body)}, nil
}
