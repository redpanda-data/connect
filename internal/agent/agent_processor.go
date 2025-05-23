/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/redpanda-data/benthos/v4/public/service"
	agentruntimepb "github.com/redpanda-data/connect/v4/internal/agent/runtimepb"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin/subprocess"
)

const (
	apFieldCmd           = "command"
	apFieldMCPServerAddr = "mcp_server"
	apFieldCWD           = "cwd"
)

func newAgentProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Fields(
			service.NewStringListField(apFieldCmd),
			service.NewStringField(apFieldMCPServerAddr),
			service.NewStringField(apFieldCWD),
		)
}

type agentProcessor struct {
	client *rpcClient
	proc   *subprocess.Subprocess
}

var _ service.Processor = (*agentProcessor)(nil)

func newAgentProcessor(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
	cmd, err := conf.FieldStringList(apFieldCmd)
	if err != nil {
		return nil, err
	}
	if len(cmd) == 0 {
		return nil, errors.New("command must be specified")
	}
	mcpServerAddress, err := conf.FieldString(apFieldMCPServerAddr)
	if err != nil {
		return nil, err
	}
	cwd, err := conf.FieldString(apFieldCWD)
	if err != nil {
		return nil, err
	}

	// TODO: Remove this junk compatibility with the hashicorp plugin stuff, and instead
	// just use a unix socket.
	protocol := make(chan string, 1)
	proc, err := subprocess.New(
		cmd,
		environMap(mcpServerAddress),
		subprocess.WithLogger(res.Logger()),
		subprocess.WithCwd(cwd),
		subprocess.WithStdoutHook(func() func(string) {
			done := false
			return func(line string) {
				if done {
					return
				}
				done = true
				protocol <- line
			}
		}()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create plugin process: %w", err)
	}
	if err := proc.Start(); err != nil {
		return nil, fmt.Errorf("failed to start plugin process: %w", err)
	}
	select {
	case line := <-protocol:
		parts := strings.Split(strings.TrimSpace(line), "|")
		if len(parts) != 5 {
			res.Logger().Debugf("missing protocol line: %q", line)
			_ = proc.Close(context.Background())
			return nil, fmt.Errorf("invalid protocol line: %q, if you're seeing this it's likely you're not calling `redpanda.runtime.serve` in your script. Do not log or print anything before this runs. If you need to make sure it goes to stderr instead of stdout", line)
		}
		if parts[0] != "1" || parts[1] != "1" || parts[2] != "tcp" || parts[4] != "grpc" {
			res.Logger().Debugf("invalid protocol line: %q", line)
			_ = proc.Close(context.Background())
			return nil, fmt.Errorf("invalid protocol line: %q, if you're seeing this it's likely you're not calling `redpanda.runtime.serve` in your script. Do not log or print anything before this runs. If you need to make sure it goes to stderr instead of stdout", line)
		}
		addr := parts[3]
		runtimeConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			res.Logger().Debugf("failed to create connection: %v", err)
			_ = proc.Close(context.Background())
			return nil, fmt.Errorf("failed to connect to plugin process: %w", err)
		}
		res.Logger().Debugf("started agent listening on %s", addr)
		client := &rpcClient{
			client: agentruntimepb.NewAgentRuntimeClient(runtimeConn),
			tracer: res.OtelTracer().Tracer("rpcn-agent"),
		}
		return &agentProcessor{
			client: client,
			proc:   proc,
		}, nil
	case <-time.After(10 * time.Second):
		res.Logger().Debugf("failed to start agent after 10 seconds")
		_ = proc.Close(context.Background())
		if !proc.IsRunning() {
			return nil, errors.New("failed to start plugin process, process exited, make sure you're calling `redpanda.runtime.serve`")
		}
		return nil, errors.New("failed to start plugin process, timeout waiting for protocol line")
	}
}

func environMap(mcpServerAddress string) map[string]string {
	m := make(map[string]string)
	for _, val := range os.Environ() {
		kv := strings.SplitN(val, "=", 2)
		m[kv[0]] = kv[1]
	}
	m["REDPANDA_CONNECT_AGENT_RUNTIME_MCP_SERVER"] = mcpServerAddress
	return m
}

// Process implements service.Processor.
func (a *agentProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	msg, err := a.client.InvokeAgent(ctx, msg)
	if err != nil {
		return nil, err
	}
	return service.MessageBatch{msg}, nil
}

// Close implements service.BatchProcessor.
func (p *agentProcessor) Close(ctx context.Context) error {
	if err := p.proc.Close(ctx); err != nil {
		return fmt.Errorf("unable to close plugin process: %w", err)
	}
	return nil
}
