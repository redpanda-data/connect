/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

//go:generate protoc -I=../../proto/redpanda/runtime/v1alpha1 --go_out=../.. --go-grpc_out=../.. runtime.proto

package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-plugin"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/agent/runtimepb"
	"google.golang.org/grpc"
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

type runtime interface {
	InvokeAgent(ctx context.Context, msg *service.Message) (*service.Message, error)
}

type runtimePlugin struct {
	plugin.NetRPCUnsupportedPlugin
	Impl runtime
}

var _ plugin.GRPCPlugin = (*runtimePlugin)(nil)
var _ plugin.Plugin = (*runtimePlugin)(nil)

// GRPCClient implements plugin.GRPCPlugin.
func (p *runtimePlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return &rpcClient{client: runtimepb.NewRuntimeClient(c)}, nil
}

// GRPCServer implements plugin.GRPCPlugin.
func (p *runtimePlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	return errors.ErrUnsupported
}

type rpcClient struct{ client runtimepb.RuntimeClient }

var _ runtime = (*rpcClient)(nil)

func (m *rpcClient) InvokeAgent(ctx context.Context, msg *service.Message) (*service.Message, error) {
	pb, err := runtimepb.MessageToProto(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to convert message for agent: %w", err)
	}
	resp, err := m.client.InvokeAgent(ctx, &runtimepb.InvokeAgentRequest{
		Message: pb,
	})
	if err != nil {
		// TODO: Support typed errors handled in the core engine
		return nil, fmt.Errorf("failed to invoke agent: %w", err)
	}
	msg, err = runtimepb.ProtoToMessage(resp.GetMessage())
	if err != nil {
		return nil, fmt.Errorf("failed to convert message from agent: %w", err)
	}
	return msg, nil
}
