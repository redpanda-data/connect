// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"strings"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/mark3labs/mcp-go/server"
	"go.opentelemetry.io/otel/propagation"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/gateway"
	"github.com/redpanda-data/connect/v4/internal/license"
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

type corsConfig struct {
	enabled        bool
	allowedOrigins []string
}

func (conf corsConfig) WrapHandler(handler http.Handler) http.Handler {
	if !conf.enabled {
		return handler
	}
	return handlers.CORS(
		handlers.AllowedOrigins(conf.allowedOrigins),
		handlers.AllowedHeaders([]string{"Content-Type", "Authorization"}),
		handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}),
	)(handler)
}

const (
	rpEnvCorsOrigins = "REDPANDA_CLOUD_GATEWAY_CORS_ORIGINS"
)

// Server runs an mcp server against a target directory, with an optiona base
// URL for an HTTP server.
type Server struct {
	base  *server.MCPServer
	mux   *mux.Router
	rpJWT *gateway.RPJWTMiddleware
	cors  corsConfig
}

// NewServer initializes the MCP server.
func NewServer(
	repositoryDir string,
	logger *slog.Logger,
	envVarLookupFunc func(context.Context, string) (string, bool),
	filterFunc func(label string) bool,
	tagFilterFunc func(tags []string) bool,
	licenseConfig license.Config,
) (*Server, error) {
	// Create MCP server
	s := server.NewMCPServer(
		"Redpanda Runtime",
		"1.0.0",
	)

	mux := mux.NewRouter()

	env := service.GlobalEnvironment()

	resWrapper := tools.NewResourcesWrapper(logger, s, filterFunc, tagFilterFunc)
	resWrapper.SetEnvVarLookupFunc(envVarLookupFunc)
	resWrapper.SetHTTPMultiplexer(&gMux{m: mux})

	repoScanner := repository.NewScanner(os.DirFS(repositoryDir))

	repoScanner.OnTemplateFile(func(_ string, contents []byte) error {
		return env.RegisterTemplateYAML(string(contents))
	})

	repoScanner.OnResourceFile(func(resourceType, filename string, contents []byte) error {
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

	repoScanner.OnMetricsFile(func(_ string, contents []byte) error {
		// TODO: Detect starlark here?
		return resWrapper.SetMetricsYAML(contents)
	})

	repoScanner.OnTracerFile(func(_ string, contents []byte) error {
		// TODO: Detect starlark here?
		return resWrapper.SetTracerYAML(contents)
	})

	if err := repoScanner.Scan("."); err != nil {
		return nil, err
	}

	resources, err := resWrapper.Build()
	if err != nil {
		return nil, err
	}

	license.RegisterService(resources, licenseConfig)

	rpJWT, err := gateway.NewRPJWTMiddleware(resources)
	if err != nil {
		return nil, err
	}

	var cors corsConfig
	if v := os.Getenv(rpEnvCorsOrigins); v != "" {
		cors.enabled = true
		cors.allowedOrigins = strings.Split(v, ",")
		for i, o := range cors.allowedOrigins {
			cors.allowedOrigins[i] = strings.TrimSpace(o)
		}
	}

	return &Server{s, mux, rpJWT, cors}, nil
}

// ServeStdio attempts to run the MCP server in stdio mode.
func (m *Server) ServeStdio() error {
	if err := server.ServeStdio(m.base); err != nil {
		return err
	}
	return nil
}

func (m *Server) addSSEEndpoints() {
	sseServer := server.NewSSEServer(
		m.base,
		server.WithSSEContextFunc(func(ctx context.Context, r *http.Request) context.Context {
			// Propagate tracing using the traceparent header from the request to the handlers in the MCP server.
			w3cTraceContext := propagation.TraceContext{}
			ctx = w3cTraceContext.Extract(ctx, propagation.HeaderCarrier(r.Header))
			return ctx
		}),
	)

	m.mux.PathPrefix("/sse").Handler(sseServer)
	m.mux.PathPrefix("/message").Handler(sseServer)
}

func (m *Server) addStreamableEndpoints() {
	streamableServer := server.NewStreamableHTTPServer(
		m.base,
		server.WithHTTPContextFunc(func(ctx context.Context, r *http.Request) context.Context {
			// Propagate tracing using the traceparent header from the request to the handlers in the MCP server.
			w3cTraceContext := propagation.TraceContext{}
			ctx = w3cTraceContext.Extract(ctx, propagation.HeaderCarrier(r.Header))
			return ctx
		}),
	)

	m.mux.PathPrefix("/mcp").Handler(streamableServer)
}

// ServeHTTP attempts to run the MCP server over HTTP.
func (m *Server) ServeHTTP(ctx context.Context, l net.Listener) error {
	m.addSSEEndpoints()
	m.addStreamableEndpoints()

	srv := &http.Server{
		Handler: m.rpJWT.Wrap(m.cors.WrapHandler(m.mux)),
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
