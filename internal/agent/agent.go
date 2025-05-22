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
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/mcp"
)

type agentConfig struct {
	Input   yaml.Node `yaml:"input"`
	Tools   []string  `yaml:"tools"`
	Output  yaml.Node `yaml:"output"`
	Tracer  yaml.Node `yaml:"tracer"`
	Metrics yaml.Node `yaml:"metrics"`
	Logger  yaml.Node `yaml:"logger"`
}

type httpConfig struct {
	enabled bool   `yaml:"enabled"`
	address string `yaml:"address"`
}

type agentsConfig struct {
	Agents map[string]agentConfig `yaml:"agents"`
	HTTP   httpConfig             `yaml:"http"`
}

type gMux struct {
	m      *mux.Router
	prefix string
}

func (g *gMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	g.m.Path(g.prefix + pattern).HandlerFunc(handler) // TODO: PathPrefix?
}

// RunAgent attempts to run an agent pipeline.
func RunAgent(
	logger *slog.Logger,
	envVarLookupFunc func(context.Context, string) (string, bool),
	repositoryDir string,
	licenseConfig license.Config,
) error {
	redpandaAgentsContents, err := os.ReadFile(filepath.Join(repositoryDir, "redpanda_agents.yaml"))
	if err != nil {
		return fmt.Errorf("failed to read redpanda_agents.yaml (are you in the right directory?): %w", err)
	}
	var config agentsConfig
	config.HTTP.enabled = true
	config.HTTP.address = "0.0.0.0:4195"
	if err := yaml.Unmarshal(redpandaAgentsContents, &config); err != nil {
		return fmt.Errorf("failed to unmarshal redpanda_agents.yaml: %w", err)
	}
	env := service.NewEnvironment()
	err = env.RegisterProcessor(
		"redpanda_agent_runtime",
		newAgentProcessorConfigSpec(),
		newAgentProcessor,
	)
	if err != nil {
		return err
	}
	mux := mux.NewRouter()
	ctx, cancel := context.WithCancelCause(context.Background())
	eg, ctx := errgroup.WithContext(ctx)
	buildStream := func(name string, agent agentConfig) (*service.Stream, error) {
		server, err := mcp.NewServer(
			filepath.Join(repositoryDir, "mcp"),
			logger,
			envVarLookupFunc,
			func(label string) bool {
				return slices.Contains(agent.Tools, label)
			},
			nil,
			licenseConfig,
		)
		if err != nil {
			return nil, err
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		go func() {
			err := server.ServeSSE(ctx, l)
			cancel(err)
			_ = l.Close()
		}()
		b := env.NewStreamBuilder()
		b.SetHTTPMux(&gMux{m: mux, prefix: "/" + name})
		b.SetLogger(logger)
		b.SetEnvVarLookupFunc(func(key string) (string, bool) {
			return envVarLookupFunc(context.Background(), key)
		})
		configs := []struct {
			name    string
			node    yaml.Node
			builder func(string) error
		}{
			{
				name:    "input",
				node:    agent.Input,
				builder: b.AddInputYAML,
			},
			{
				name:    "output",
				node:    agent.Output,
				builder: b.AddOutputYAML,
			},
			{
				name:    "metrics",
				node:    agent.Metrics,
				builder: b.SetMetricsYAML,
			},
			{
				name:    "logger",
				node:    agent.Logger,
				builder: b.SetLoggerYAML,
			},
			{
				name:    "tracer",
				node:    agent.Tracer,
				builder: b.SetTracerYAML,
			},
		}
		for _, config := range configs {
			if !config.node.IsZero() {
				str, _ := yaml.Marshal(config.node)
				if err := config.builder(string(str)); err != nil {
					return nil, fmt.Errorf("failed to add agent %s: %w", config.name, err)
				}
			}
		}
		err = b.AddProcessorYAML(strings.NewReplacer(
			"$NAME", name,
			"$PORT", strconv.Itoa(l.Addr().(*net.TCPAddr).Port),
			"$CWD", repositoryDir,
		).Replace(`
redpanda_agent_runtime:
  command: ["uv", "run", "agents/$NAME.py"]
  mcp_server: "http://127.0.0.1:$PORT/sse"
  cwd: "$CWD"
      `))
		if err != nil {
			return nil, fmt.Errorf("failed to add agent processor: %w", err)
		}
		stream, err := b.Build()
		if err != nil {
			return nil, fmt.Errorf("failed to add build agent stream: %w", err)
		}
		return stream, nil
	}
	for name, agent := range config.Agents {
		stream, err := buildStream(name, agent)
		if err != nil {
			eg.Go(func() error { return err })
			cancel(err)
			break
		}
		license.RegisterService(
			stream.Resources(),
			licenseConfig,
		)
		eg.Go(func() error { return stream.Run(ctx) })
	}
	if config.HTTP.enabled {
		srv := &http.Server{Addr: config.HTTP.address, Handler: mux}
		eg.Go(func() error {
			err := srv.ListenAndServe()
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			return err
		})
		eg.Go(func() error {
			<-ctx.Done()
			return srv.Shutdown(context.Background())
		})
	}
	err = eg.Wait()
	cancel(err)
	return err
}
