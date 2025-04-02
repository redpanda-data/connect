/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package mcp

import (
	"context"
	"log/slog"
	"net"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

// Run an mcp server against a target directory, with an optional base URL for
// an HTTP server.
func Run(
	logger *slog.Logger,
	envVarLookupFunc func(context.Context, string) (string, bool),
	repositoryDir, addr string,
) error {
	srv, err := NewMCPServer(repositoryDir, logger, envVarLookupFunc, func(string) bool { return true })
	if err != nil {
		return err
	}
	if addr == "" {
		return srv.ServeStdio()
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	return srv.ServeSSE(context.Background(), l)
}
