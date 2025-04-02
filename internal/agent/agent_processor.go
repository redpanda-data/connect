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
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/redpanda-data/benthos/v4/public/service"
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
	client  *plugin.Client
	runtime runtime
	once    sync.Once
}

var _ service.Processor = (*agentProcessor)(nil)

func newAgentProcessor(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
	cmd, err := conf.FieldStringList(apFieldCmd)
	if err != nil {
		return nil, err
	}
	if len(cmd) == 0 {
		return nil, fmt.Errorf("command must be specified")
	}
	mcpServerAddress, err := conf.FieldString(apFieldMCPServerAddr)
	if err != nil {
		return nil, err
	}
	cwd, err := conf.FieldString(apFieldCWD)
	if err != nil {
		return nil, err
	}
	c := exec.Command(cmd[0], cmd[1:]...)
	c.Dir = cwd
	c.Env = append(os.Environ(), "REDPANDA_CONNECT_AGENT_RUNTIME_MCP_SERVER="+mcpServerAddress)
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  handshake,
		Plugins:          pluginMap,
		Cmd:              c,
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		SkipHostEnv:      true, // We do this ourselves
		Logger:           &hclogger{res.Logger()},
	})
	rpc, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, err
	}
	raw, err := rpc.Dispense("runtime")
	if err != nil {
		client.Kill()
		return nil, err
	}
	rt := raw.(runtime)
	return &agentProcessor{
		client:  client,
		runtime: rt,
	}, nil
}

// Process implements service.Processor.
func (a *agentProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	msg, err := a.runtime.InvokeAgent(ctx, msg)
	if err != nil {
		return nil, err
	}
	return service.MessageBatch{msg}, nil
}

// Close implements service.Processor.
func (a *agentProcessor) Close(ctx context.Context) error {
	a.once.Do(a.client.Kill)
	return nil
}

type hclogger struct {
	logger *service.Logger
}

// Debug implements hclog.Logger.
func (h *hclogger) Debug(msg string, args ...any) {
	for i := 0; i < len(args); i = i + 2 {
		msg = fmt.Sprintf("%s %s=%v", msg, args[i], args[i+1])
	}
	h.logger.Debug(msg)
}

// Error implements hclog.Logger.
func (h *hclogger) Error(msg string, args ...any) {
	for i := 0; i < len(args); i = i + 2 {
		msg = fmt.Sprintf("%s %s=%v", msg, args[i], args[i+1])
	}
	h.logger.Error(msg)
}

// GetLevel implements hclog.Logger.
func (h *hclogger) GetLevel() hclog.Level {
	return hclog.Info
}

// ImpliedArgs implements hclog.Logger.
func (h *hclogger) ImpliedArgs() []any {
	return nil
}

// Info implements hclog.Logger.
func (h *hclogger) Info(msg string, args ...any) {
	for i := 0; i < len(args); i = i + 2 {
		msg = fmt.Sprintf("%s %s=%v", msg, args[i], args[i+1])
	}
	h.logger.Info(msg)
}

// IsDebug implements hclog.Logger.
func (h *hclogger) IsDebug() bool {
	return false
}

// IsError implements hclog.Logger.
func (h *hclogger) IsError() bool {
	return true
}

// IsInfo implements hclog.Logger.
func (h *hclogger) IsInfo() bool {
	return true
}

// IsTrace implements hclog.Logger.
func (h *hclogger) IsTrace() bool {
	return false
}

// IsWarn implements hclog.Logger.
func (h *hclogger) IsWarn() bool {
	return true
}

// Log implements hclog.Logger.
func (h *hclogger) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Debug:
		h.Debug(msg, args...)
	case hclog.Error:
		h.Error(msg, args...)
	case hclog.Info:
		h.Info(msg, args...)
	case hclog.Trace:
		h.Trace(msg, args...)
	case hclog.Warn:
		h.Warn(msg, args...)
	case hclog.NoLevel:
	case hclog.Off:
	}
}

// Name implements hclog.Logger.
func (h *hclogger) Name() string {
	return "redpanda-connect-plugin"
}

// Named implements hclog.Logger.
func (h *hclogger) Named(name string) hclog.Logger {
	return h
}

// ResetNamed implements hclog.Logger.
func (h *hclogger) ResetNamed(name string) hclog.Logger {
	return h
}

// SetLevel implements hclog.Logger.
func (h *hclogger) SetLevel(level hclog.Level) {
}

// StandardLogger implements hclog.Logger.
func (h *hclogger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(io.Discard, "", 0)
}

// StandardWriter implements hclog.Logger.
func (h *hclogger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return io.Discard
}

// Trace implements hclog.Logger.
func (h *hclogger) Trace(msg string, args ...any) {
	for i := 0; i < len(args); i = i + 2 {
		msg = fmt.Sprintf("%s %s=%v", msg, args[i], args[i+1])
	}
	h.logger.Trace(msg)
}

// Warn implements hclog.Logger.
func (h *hclogger) Warn(msg string, args ...any) {
	for i := 0; i < len(args); i = i + 2 {
		msg = fmt.Sprintf("%s %s=%v", msg, args[i], args[i+1])
	}
	h.logger.Warn(msg)
}

// With implements hclog.Logger.
func (h *hclogger) With(args ...any) hclog.Logger {
	return h
}
