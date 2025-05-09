/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

//go:generate protoc -I=../../proto --go-grpc_opt=module=github.com/redpanda-data/connect/v4 --go_opt=module=github.com/redpanda-data/connect/v4 --go_out=../.. --go-grpc_out=../.. redpanda/runtime/v1alpha1/agent.proto

package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-plugin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	agentruntimepb "github.com/redpanda-data/connect/v4/internal/agent/runtimepb"
	"github.com/redpanda-data/connect/v4/internal/dynamic/plugin/runtimepb"
	"github.com/redpanda-data/connect/v4/internal/tracing"
)

// Handshake is a common handshake that is shared by plugin and host.
var handshake = plugin.HandshakeConfig{
	// This isn't required when using VersionedPlugins
	ProtocolVersion:  1,
	MagicCookieKey:   "REDPANDA_CONNECT_DYNAMIC_PLUGIN",
	MagicCookieValue: ":blobswag:",
}

// PluginMap is the map of plugins we can dispense.
var pluginMap = map[string]plugin.Plugin{
	"runtime": &runtimePlugin{},
}

type runtimePlugin struct {
	plugin.NetRPCUnsupportedPlugin
}

var (
	_ plugin.GRPCPlugin = (*runtimePlugin)(nil)
	_ plugin.Plugin     = (*runtimePlugin)(nil)
)

// GRPCClient implements plugin.GRPCPlugin.
func (p *runtimePlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return &rpcClient{
		client: agentruntimepb.NewAgentRuntimeClient(c),
		tracer: nil,
	}, nil
}

// GRPCServer implements plugin.GRPCPlugin.
func (p *runtimePlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	return errors.ErrUnsupported
}

type rpcClient struct {
	client agentruntimepb.AgentRuntimeClient
	tracer trace.Tracer
}

func (m *rpcClient) InvokeAgent(ctx context.Context, inputMsg *service.Message) (*service.Message, error) {
	pb, err := runtimepb.MessageToProto(inputMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to convert message for agent: %w", err)
	}
	span := trace.SpanFromContext(inputMsg.Context())
	var traceContext *agentruntimepb.TraceContext
	if c := span.SpanContext(); c.IsValid() {
		traceContext = &agentruntimepb.TraceContext{
			TraceId:    c.TraceID().String(),
			SpanId:     c.SpanID().String(),
			TraceFlags: c.TraceFlags().String(),
		}
	}

	resp, err := m.client.InvokeAgent(ctx, &agentruntimepb.InvokeAgentRequest{
		Message:      pb,
		TraceContext: traceContext,
	})
	if err != nil {
		// TODO: Support typed errors handled in the core engine
		return nil, fmt.Errorf("failed to invoke agent: %w", err)
	}
	outputMsg, err := runtimepb.ProtoToMessage(resp.GetMessage())
	if err != nil {
		return nil, fmt.Errorf("failed to convert message from agent: %w", err)
	}
	// Copy the context too
	outputMsg = outputMsg.WithContext(inputMsg.Context())
	if err := m.applySubSpans(outputMsg.Context(), resp.GetTrace().GetSpans()); err != nil {
		return nil, err
	}
	return outputMsg, nil
}

func (m *rpcClient) applySubSpans(ctx context.Context, spans []*agentruntimepb.Span) error {
	for _, protoSpan := range spans {
		var attrs []attribute.KeyValue
		for k, v := range protoSpan.GetAttributes() {
			kv, err := valueToAttribute(attribute.Key(k), v)
			if err != nil {
				return fmt.Errorf("unable to convert tracing attribute %q: %w", k, err)
			}
			attrs = append(attrs, kv)
		}
		spanID, err := trace.SpanIDFromHex(protoSpan.GetSpanId())
		if err != nil {
			return fmt.Errorf("unable to parse span id %q: %w", protoSpan.GetSpanId(), err)
		}
		subCtx, otelSpan := m.tracer.Start(
			tracing.WithCustomSpanID(ctx, spanID),
			protoSpan.GetName(),
			trace.WithTimestamp(protoSpan.GetStartTime().AsTime()),
			trace.WithAttributes(attrs...),
		)
		err = m.applySubSpans(subCtx, protoSpan.GetChildSpans())
		otelSpan.End(trace.WithTimestamp(protoSpan.GetEndTime().AsTime()))
		if err != nil {
			return err
		}
	}
	return nil
}

func valueToAttribute(key attribute.Key, val *runtimepb.Value) (attribute.KeyValue, error) {
	switch v := val.Kind.(type) {
	case *runtimepb.Value_BoolValue:
		return key.Bool(v.BoolValue), nil
	case *runtimepb.Value_IntegerValue:
		return key.Int64(v.IntegerValue), nil
	case *runtimepb.Value_DoubleValue:
		return key.Float64(v.DoubleValue), nil
	case *runtimepb.Value_StringValue:
		return key.String(v.StringValue), nil
	case *runtimepb.Value_NullValue,
		*runtimepb.Value_BytesValue,
		*runtimepb.Value_TimestampValue,
		*runtimepb.Value_ListValue,
		*runtimepb.Value_StructValue:
		// Fallback to JSON serialization, althrough it might be possible for certain
		// lists to be converted to high level types.
		val, err := runtimepb.ValueToAny(val)
		if err != nil {
			return attribute.KeyValue{}, err
		}
		return key.String(bloblang.ValueToString(val)), nil
	}
	return attribute.KeyValue{}, fmt.Errorf("unsupported type: %T", val.Kind)
}
