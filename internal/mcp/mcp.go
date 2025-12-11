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

	"github.com/gorilla/mux"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/propagation"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/gateway"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/mcp/metrics"
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
	base      *mcp.Server
	mux       *mux.Router
	rpJWT     *gateway.RPJWTMiddleware
	cors      gateway.CORSConfig
	resources *service.Resources
}

// NewServer initializes the MCP server.
func NewServer(
	repositoryDir string,
	logger *slog.Logger,
	envVarLookupFunc func(context.Context, string) (string, bool),
	filterFunc func(label string) bool,
	tagFilterFunc func(tags []string) bool,
	licenseConfig license.Config,
	auth *Authorizer,
) (*Server, error) {
	// Create MCP server
	s := mcp.NewServer(&mcp.Implementation{
		Name:    "Redpanda Runtime",
		Version: "1.0.0",
	}, nil)

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

	// Add metrics middleware to track all MCP method calls
	mcpMetrics := metrics.NewMetrics(resources.Metrics())
	s.AddReceivingMiddleware(mcpMetrics.ReceivingMiddleware)
	s.AddSendingMiddleware(mcpMetrics.SendingMiddleware)

	if auth != nil {
		if err := license.CheckRunningEnterprise(resources); err != nil {
			return nil, fmt.Errorf("unable to apply authorization policy: %w", err)
		}
		s.AddReceivingMiddleware(auth.Middleware)
	}

	rpJWT, err := gateway.NewRPJWTMiddleware(resources)
	if err != nil {
		return nil, err
	}

	cors := gateway.NewCORSConfigFromEnv()

	return &Server{
		base:      s,
		mux:       mux,
		rpJWT:     rpJWT,
		cors:      cors,
		resources: resources,
	}, nil
}

// Resources returns the server's service resources for testing purposes.
func (m *Server) Resources() *service.Resources {
	return m.resources
}

// ServeStdio attempts to run the MCP server in stdio mode.
func (m *Server) ServeStdio() error {
	return m.base.Run(context.Background(), &mcp.StdioTransport{})
}

func (m *Server) addSSEEndpoints() {
	sseHandler := mcp.NewSSEHandler(func(_ *http.Request) *mcp.Server {
		return m.base
	}, nil)

	// Wrap the handler to propagate tracing from traceparent header
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Propagate tracing using the traceparent header from the request
		w3cTraceContext := propagation.TraceContext{}
		ctx := w3cTraceContext.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		r = r.WithContext(ctx)
		sseHandler.ServeHTTP(w, r)
	})

	m.mux.PathPrefix("/sse").Handler(wrappedHandler)
	m.mux.PathPrefix("/message").Handler(wrappedHandler)
}

func (m *Server) addStreamableEndpoints() {
	streamableHandler := mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server {
		return m.base
	}, nil)

	// Wrap the handler to propagate tracing from traceparent header
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Propagate tracing using the traceparent header from the request
		w3cTraceContext := propagation.TraceContext{}
		ctx := w3cTraceContext.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		r = r.WithContext(ctx)
		streamableHandler.ServeHTTP(w, r)
	})

	m.mux.PathPrefix("/mcp").Handler(wrappedHandler)
}

// ServeHTTP attempts to run the MCP server over HTTP.
func (m *Server) ServeHTTP(ctx context.Context, l net.Listener) error {
	m.addSSEEndpoints()
	m.addStreamableEndpoints()

	srv := &http.Server{
		Handler: m.cors.WrapHandler(m.rpJWT.Wrap(m.mux)),
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
