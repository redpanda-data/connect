// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package host implements the v1 plugin-runtime host adapter. It spawns
// a plugin subprocess, performs the Handshake + ListComponents
// exchange, and registers remote-proxy constructors against a
// *service.Environment so plugin-provided components look identical to
// in-tree components to the rest of the benthos runtime.
//
// Intended call site:
//
//	plugin, err := host.RegisterPluginBinary(env, []string{"./mybin"},
//	    host.WithLogger(res.Logger()),
//	)
//	if err != nil { /* ... */ }
//	defer plugin.Close(ctx)
//
// One subprocess can register any number of components — they all share
// the single gRPC connection multiplexed by instance_id.
package host

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/rpcplugin/subprocess"
	runtimepb "github.com/redpanda-data/connect/v4/internal/rpcplugin/v1/runtimepb"
)

const (
	// hostProtocolVersion is the wire-protocol version this host
	// adapter can speak. Reported to plugins during Handshake.
	hostProtocolVersion = 1

	maxStartupTime    = 30 * time.Second
	maxStartupTimeCI  = 120 * time.Second
	hostHandshakeName = "redpanda-connect"
)

// Plugin is the live handle to a running plugin subprocess. Closing it
// shuts down the gRPC connection and the child process.
type Plugin struct {
	cmd        []string
	subprocess *subprocess.Subprocess
	conn       *grpc.ClientConn
	client     runtimepb.PluginRuntimeClient
	logger     *service.Logger
	components []ComponentInfo
}

// ComponentInfo describes a single component the plugin exposes.
type ComponentInfo struct {
	Name string
	Kind runtimepb.ComponentKind
}

// options configures a plugin registration.
type options struct {
	logger     *service.Logger
	cwd        string
	env        map[string]string
	stderrHook func(string)
	stdoutHook func(string)
}

// Option configures RegisterPluginBinary.
type Option func(*options)

// WithLogger overrides the logger used for plugin subprocess
// management. Required — the underlying subprocess wrapper panics on a
// nil logger.
func WithLogger(l *service.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithCwd sets the working directory the subprocess inherits.
func WithCwd(dir string) Option {
	return func(o *options) { o.cwd = dir }
}

// WithEnv adds environment variables to the subprocess environment.
// REDPANDA_CONNECT_PLUGIN_ADDRESS is added automatically and cannot be
// overridden.
func WithEnv(env map[string]string) Option {
	return func(o *options) {
		if o.env == nil {
			o.env = make(map[string]string)
		}
		maps.Copy(o.env, env)
	}
}

// WithStderrHook forwards each line written to the subprocess's stderr
// to the supplied callback.
func WithStderrHook(hook func(line string)) Option {
	return func(o *options) { o.stderrHook = hook }
}

// WithStdoutHook forwards each line written to the subprocess's stdout
// to the supplied callback. Plugin stdout is typically reserved for
// the gRPC handshake; this is mainly useful for debugging.
func WithStdoutHook(hook func(line string)) Option {
	return func(o *options) { o.stdoutHook = hook }
}

// RegisterPluginBinary launches a plugin subprocess, discovers its
// components via the PluginRuntime protocol, and registers each one
// against the supplied *service.Environment via a remote-proxy
// constructor. The returned *Plugin owns the subprocess and gRPC
// connection; the caller is responsible for calling Close when done.
//
// PoC limitation: only processor components are registered against
// env. Inputs/outputs are still reported in the returned
// ComponentInfo slice for visibility but are skipped during
// registration.
func RegisterPluginBinary(env *service.Environment, cmd []string, opts ...Option) (*Plugin, error) {
	if env == nil {
		return nil, errors.New("v1 host: nil *service.Environment")
	}
	if len(cmd) == 0 {
		return nil, errors.New("v1 host: empty plugin command")
	}

	o := &options{
		env: make(map[string]string),
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.logger == nil {
		return nil, errors.New("v1 host: WithLogger is required")
	}

	socketPath, err := newUnixSocketAddr()
	if err != nil {
		return nil, fmt.Errorf("allocating socket: %w", err)
	}

	cleanup := []func(){}
	defer func() {
		for _, fn := range cleanup {
			fn()
		}
	}()

	conn, err := grpc.NewClient(
		socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating gRPC client: %w", err)
	}
	cleanup = append(cleanup, func() { _ = conn.Close() })

	o.env["REDPANDA_CONNECT_PLUGIN_ADDRESS"] = socketPath
	proc, err := subprocess.New(
		cmd,
		o.env,
		subprocess.WithLogger(o.logger),
		subprocess.WithCwd(o.cwd),
		subprocess.WithStderrHook(o.stderrHook),
		subprocess.WithStdoutHook(o.stdoutHook),
	)
	if err != nil {
		return nil, fmt.Errorf("creating subprocess: %w", err)
	}
	if err := proc.Start(); err != nil {
		return nil, fmt.Errorf("starting subprocess: %w", err)
	}
	cleanup = append(cleanup, func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = proc.Close(closeCtx)
	})

	startCtx, cancel := context.WithTimeout(context.Background(), startupBudget())
	defer cancel()

	client := runtimepb.NewPluginRuntimeClient(conn)

	// Wait for the plugin to come up by repeatedly calling Handshake.
	handshake, err := waitForHandshake(startCtx, client, proc)
	if err != nil {
		return nil, fmt.Errorf("handshake: %w", err)
	}
	if handshake.GetSelectedProtocolVersion() != hostProtocolVersion {
		return nil, fmt.Errorf("handshake: plugin selected unsupported protocol version %d", handshake.GetSelectedProtocolVersion())
	}
	o.logger.Debugf("v1 plugin handshake ok: sdk=%s/%s lang=%s",
		handshake.GetSdkName(), handshake.GetSdkVersion(), handshake.GetLanguage())

	// Discover and register components.
	listResp, err := client.ListComponents(startCtx, &runtimepb.ListComponentsRequest{})
	if err != nil {
		return nil, fmt.Errorf("ListComponents: %w", err)
	}

	plugin := &Plugin{
		cmd:        cmd,
		subprocess: proc,
		conn:       conn,
		client:     client,
		logger:     o.logger,
	}
	for _, comp := range listResp.GetComponents() {
		plugin.components = append(plugin.components, ComponentInfo{
			Name: comp.GetName(),
			Kind: comp.GetKind(),
		})

		switch comp.GetKind() {
		case runtimepb.ComponentKind_COMPONENT_KIND_PROCESSOR:
			if err := registerProcessor(env, client, comp); err != nil {
				return nil, fmt.Errorf("registering processor %q: %w", comp.GetName(), err)
			}
		default:
			o.logger.Debugf("skipping registration of component %q kind %v — PoC supports processors only",
				comp.GetName(), comp.GetKind())
		}
	}

	cleanup = nil // ownership transferred to the returned *Plugin
	return plugin, nil
}

// registerProcessor wires up one processor component on the host's
// env.
func registerProcessor(
	env *service.Environment,
	client runtimepb.PluginRuntimeClient,
	comp *runtimepb.ComponentDescriptor,
) error {
	spec, err := protoToServiceSpec(comp.GetSpec())
	if err != nil {
		return fmt.Errorf("rebuilding spec: %w", err)
	}
	name := comp.GetName()
	ctor := func(parsed *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
		cfg, err := parsed.FieldAny()
		if err != nil {
			return nil, fmt.Errorf("extracting config: %w", err)
		}
		cfg, err = coerceConfigValue(cfg)
		if err != nil {
			return nil, fmt.Errorf("normalising config: %w", err)
		}
		// PoC: per-instance context is the caller's. A future iteration
		// will plumb the resources context through to remote calls.
		ctx, cancel := context.WithTimeout(context.Background(), maxStartupTime)
		defer cancel()
		return newRemoteBatchProcessor(ctx, client, name, cfg)
	}
	return env.RegisterBatchProcessor(name, spec, ctor)
}

// Components returns metadata for every component the plugin exposed.
func (p *Plugin) Components() []ComponentInfo {
	out := make([]ComponentInfo, len(p.components))
	copy(out, p.components)
	return out
}

// Close shuts down the gRPC connection and the subprocess. Safe to
// call multiple times.
func (p *Plugin) Close(ctx context.Context) error {
	var firstErr error
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			firstErr = fmt.Errorf("closing gRPC conn: %w", err)
		}
		p.conn = nil
	}
	if p.subprocess != nil {
		if err := p.subprocess.Close(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing subprocess: %w", err)
		}
		p.subprocess = nil
	}
	return firstErr
}

// ----------------------------------------------------------------------
// helpers (mirrors of internal/rpcplugin/util.go; duplicated rather
// than exported to keep v0 and v1 independent during the RFC phase)
// ----------------------------------------------------------------------

func newUnixSocketAddr() (string, error) {
	dir, err := os.MkdirTemp(os.TempDir(), "rpcn_v1_plugin_*")
	if err != nil {
		return "", fmt.Errorf("creating temp dir: %w", err)
	}
	return "unix:" + filepath.Join(dir, "plugin.sock"), nil
}

func startupBudget() time.Duration {
	if os.Getenv("CI") != "" {
		return maxStartupTimeCI
	}
	return maxStartupTime
}

// coerceConfigValue normalises the result of ParsedConfig.FieldAny()
// into a plain-Go tree (map[string]any / []any / scalars). Benthos
// sometimes returns *yaml.Node for fields without a structured
// representation in the spec (e.g. empty objects); the wire-form
// AnyToProto helper doesn't recognise yaml.Node, so we decode here.
func coerceConfigValue(v any) (any, error) {
	if v == nil {
		return map[string]any{}, nil
	}
	node, ok := v.(*yaml.Node)
	if !ok {
		return v, nil
	}
	var out any
	if err := node.Decode(&out); err != nil {
		return nil, fmt.Errorf("decoding yaml.Node config: %w", err)
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

func waitForHandshake(
	ctx context.Context,
	client runtimepb.PluginRuntimeClient,
	_ *subprocess.Subprocess,
) (*runtimepb.HandshakeResponse, error) {
	// We deliberately do NOT consult subprocess.IsRunning here: that
	// method races with the wait goroutine on the underlying *exec.Cmd
	// (subprocess/subprocess.go:176 vs subprocess.go:135). Instead we
	// rely on the backoff budget to bound how long we wait — a dead
	// subprocess will simply fail the dial repeatedly until the budget
	// is exhausted.
	bo := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxInterval(2*time.Second),
		backoff.WithMaxElapsedTime(startupBudget()),
	)
	var resp *runtimepb.HandshakeResponse
	err := backoff.Retry(func() error {
		r, err := client.Handshake(ctx, &runtimepb.HandshakeRequest{
			SupportedProtocolVersions: []uint32{hostProtocolVersion},
			HostVersion:               hostHandshakeName,
		})
		if err != nil {
			return err
		}
		resp = r
		return nil
	}, bo)
	return resp, err
}
