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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/mark3labs/mcp-go/server"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/mcp/repository"
	"github.com/redpanda-data/connect/v4/internal/mcp/starlark"
	"github.com/redpanda-data/connect/v4/internal/mcp/tools"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

type gMux struct {
	m *mux.Router
}

func (g *gMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	g.m.Path(pattern).HandlerFunc(handler) // TODO: PathPrefix?
}

// Server runs an mcp server against a target directory, with an optiona base
// URL for an HTTP server.
type Server struct {
	base *server.MCPServer
	mux  *mux.Router
}

// NewServer initializes the MCP server.
func NewServer(
	repositoryDir string,
	logger *slog.Logger,
	envVarLookupFunc func(context.Context, string) (string, bool),
	filter func(label string) bool,
) (*Server, error) {
	// Create MCP server
	s := server.NewMCPServer(
		"Redpanda Runtime",
		"1.0.0",
	)

	mux := mux.NewRouter()

	env := service.GlobalEnvironment()

	resWrapper := tools.NewResourcesWrapper(logger, s, filter)
	resWrapper.SetEnvVarLookupFunc(envVarLookupFunc)
	resWrapper.SetHTTPMultiplexer(&gMux{m: mux})

	repoScanner := repository.NewScanner(os.DirFS(repositoryDir))
	repoScanner.OnResourceFile(func(resourceType string, filename string, contents []byte) error {
		switch resourceType {
		case "starlark":
			result, err := starlark.Eval(context.Background(), env, logger, filename, contents, envVarLookupFunc)
			if err != nil {
				return err
			}
			for _, v := range result.Processors {
				cfg := map[string]any{
					"label": v.Label,
					v.Name:  v.SerializedConfig,
					"meta": map[string]any{
						"mcp": map[string]any{
							"enabled":     true,
							"description": v.Description,
						},
					},
				}
				b, err := json.Marshal(&cfg)
				if err != nil {
					return err
				}
				if err := resWrapper.AddProcessorYAML(b); err != nil {
					return err
				}
			}
		case "input":
			if err := resWrapper.AddInputYAML(contents); err != nil {
				return err
			}
		case "cache":
			if err := resWrapper.AddCacheYAML(contents); err != nil {
				return err
			}
		case "processor":
			if err := resWrapper.AddProcessorYAML(contents); err != nil {
				return err
			}
		case "output":
			if err := resWrapper.AddOutputYAML(contents); err != nil {
				return err
			}
		default:
			return fmt.Errorf("resource type '%v' is not supported yet", resourceType)
		}
		return nil
	})

	repoScanner.OnMetricsFile(func(fileName string, contents []byte) error {
		// TODO: Detect starlark here?
		return resWrapper.SetMetricsYAML(contents)
	})

	if err := repoScanner.Scan("."); err != nil {
		return nil, err
	}

	if err := resWrapper.Build(); err != nil {
		return nil, err
	}

	return &Server{s, mux}, nil
}

// ServeStdio attempts to run the MCP server in stdio mode.
func (m *Server) ServeStdio() error {
	if err := server.ServeStdio(m.base); err != nil {
		return err
	}
	return nil
}

// ServeSSE attempts to run the MCP server in SSE mode.
func (m *Server) ServeSSE(ctx context.Context, l net.Listener) error {
	sseServer := server.NewSSEServer(m.base, server.WithBaseURL(
		"http://"+l.Addr().String(),
	))
	m.mux.PathPrefix("/").Handler(sseServer)
	srv := &http.Server{
		Handler: m.mux,
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	err := srv.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
