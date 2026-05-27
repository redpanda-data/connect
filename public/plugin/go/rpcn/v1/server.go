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

package rpcn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"google.golang.org/grpc"

	runtimev1 "github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb"
	runtimepb "github.com/redpanda-data/connect/v4/internal/rpcplugin/v1/runtimepb"
)

const (
	// sdkProtocolVersion is the wire-protocol version this SDK
	// speaks. Bumped on every backwards-incompatible change.
	sdkProtocolVersion = 1

	sdkName     = "rpcn-go"
	sdkVersion  = "v1.0.0-poc"
	sdkLanguage = "go"
)

// Serve starts the gRPC server that drives the PluginRuntime
// protocol. Blocks until the connection is closed or a termination
// signal is received.
//
// The function reads the listening address from the
// REDPANDA_CONNECT_PLUGIN_ADDRESS environment variable. Plugin authors
// don't construct this directly — the host sets it before exec'ing the
// subprocess.
func Serve(env *Environment) error {
	if env == nil {
		return errors.New("rpcn: nil Environment passed to Serve")
	}
	addr, ok := os.LookupEnv("REDPANDA_CONNECT_PLUGIN_ADDRESS")
	if !ok || addr == "" {
		return errors.New("rpcn: REDPANDA_CONNECT_PLUGIN_ADDRESS not set")
	}

	listener, err := listenOn(addr)
	if err != nil {
		return fmt.Errorf("rpcn: listen %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()
	runtimepb.RegisterPluginRuntimeServer(grpcServer, newServer(env))

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		grpcServer.GracefulStop()
	}()

	return grpcServer.Serve(listener)
}

// listenOn opens a listener from a REDPANDA_CONNECT_PLUGIN_ADDRESS
// string. Supported schemes: `unix://path`, `unix:path`, `tcp://host:port`.
func listenOn(addr string) (net.Listener, error) {
	switch {
	case strings.HasPrefix(addr, "unix://"):
		return net.Listen("unix", strings.TrimPrefix(addr, "unix://"))
	case strings.HasPrefix(addr, "unix:"):
		return net.Listen("unix", strings.TrimPrefix(addr, "unix:"))
	case strings.HasPrefix(addr, "tcp://"):
		return net.Listen("tcp", strings.TrimPrefix(addr, "tcp://"))
	default:
		return nil, errors.New("unknown scheme")
	}
}

// ----------------------------------------------------------------------
// gRPC service implementation
// ----------------------------------------------------------------------

type server struct {
	runtimepb.UnimplementedPluginRuntimeServer

	env *Environment

	nextInstanceID atomic.Uint64
	mu             sync.RWMutex
	instances      map[uint64]*instance
}

type instance struct {
	componentName string
	processor     BatchProcessor
	// input + output instances will land here in follow-up work.
}

func newServer(env *Environment) *server {
	return &server{
		env:       env,
		instances: make(map[uint64]*instance),
	}
}

// Handshake negotiates the protocol version. PoC always selects
// version 1.
func (*server) Handshake(_ context.Context, req *runtimepb.HandshakeRequest) (*runtimepb.HandshakeResponse, error) {
	selected, ok := pickProtocolVersion(req.GetSupportedProtocolVersions(), []uint32{sdkProtocolVersion})
	if !ok {
		return &runtimepb.HandshakeResponse{
			Error: runtimev1.ErrorToProto(fmt.Errorf(
				"no protocol version overlap: host=%v sdk=%v",
				req.GetSupportedProtocolVersions(), []uint32{sdkProtocolVersion},
			)),
		}, nil
	}
	return &runtimepb.HandshakeResponse{
		SelectedProtocolVersion: selected,
		SdkName:                 sdkName,
		SdkVersion:              sdkVersion,
		Language:                sdkLanguage,
	}, nil
}

// pickProtocolVersion returns the host's highest preference that also
// appears in the plugin's supported set.
func pickProtocolVersion(hostPrefs, sdkSupports []uint32) (uint32, bool) {
	supported := make(map[uint32]struct{}, len(sdkSupports))
	for _, v := range sdkSupports {
		supported[v] = struct{}{}
	}
	for _, v := range hostPrefs {
		if _, ok := supported[v]; ok {
			return v, true
		}
	}
	return 0, false
}

// ListComponents returns the descriptors of all components registered
// on the plugin's Environment.
func (s *server) ListComponents(_ context.Context, _ *runtimepb.ListComponentsRequest) (*runtimepb.ListComponentsResponse, error) {
	components := make([]*runtimepb.ComponentDescriptor, 0,
		len(s.env.procs)+len(s.env.inputs)+len(s.env.outputs))

	for _, p := range s.env.procs {
		components = append(components, &runtimepb.ComponentDescriptor{
			Name: p.name,
			Kind: runtimepb.ComponentKind_COMPONENT_KIND_PROCESSOR,
			Spec: p.spec.desc.toProto(),
		})
	}
	for _, in := range s.env.inputs {
		components = append(components, &runtimepb.ComponentDescriptor{
			Name: in.name,
			Kind: runtimepb.ComponentKind_COMPONENT_KIND_INPUT,
			Spec: in.spec.desc.toProto(),
		})
	}
	for _, out := range s.env.outputs {
		components = append(components, &runtimepb.ComponentDescriptor{
			Name: out.name,
			Kind: runtimepb.ComponentKind_COMPONENT_KIND_OUTPUT,
			Spec: out.spec.desc.toProto(),
		})
	}
	return &runtimepb.ListComponentsResponse{Components: components}, nil
}

// OpenInstance constructs a new component instance bound to a parsed
// config.
func (s *server) OpenInstance(_ context.Context, req *runtimepb.OpenInstanceRequest) (*runtimepb.OpenInstanceResponse, error) {
	procReg := s.lookupProcessor(req.GetComponentName())
	if procReg == nil {
		// PoC: only processor components supported.
		return &runtimepb.OpenInstanceResponse{
			Error: runtimev1.ErrorToProto(fmt.Errorf(
				"unknown component %q (PoC supports processors only)", req.GetComponentName(),
			)),
		}, nil
	}

	spec, err := descriptorToServiceSpec(procReg.spec.desc)
	if err != nil {
		return &runtimepb.OpenInstanceResponse{
			Error: runtimev1.ErrorToProto(fmt.Errorf("rebuilding spec: %w", err)),
		}, nil
	}

	configYAML, err := valueToYAML(req.GetConfig())
	if err != nil {
		return &runtimepb.OpenInstanceResponse{
			Error: runtimev1.ErrorToProto(fmt.Errorf("decoding config: %w", err)),
		}, nil
	}

	parsedConf, err := spec.ParseYAML(configYAML, nil)
	if err != nil {
		return &runtimepb.OpenInstanceResponse{
			Error: runtimev1.ErrorToProto(fmt.Errorf("parsing config: %w", err)),
		}, nil
	}

	proc, err := procReg.ctor(parsedConf, nil)
	if err != nil {
		return &runtimepb.OpenInstanceResponse{
			Error: runtimev1.ErrorToProto(fmt.Errorf("constructing component: %w", err)),
		}, nil
	}

	id := s.nextInstanceID.Add(1)
	s.mu.Lock()
	s.instances[id] = &instance{componentName: procReg.name, processor: proc}
	s.mu.Unlock()

	return &runtimepb.OpenInstanceResponse{InstanceId: id}, nil
}

// CloseInstance shuts down a previously-opened instance.
func (s *server) CloseInstance(ctx context.Context, req *runtimepb.CloseInstanceRequest) (*runtimepb.CloseInstanceResponse, error) {
	s.mu.Lock()
	inst, ok := s.instances[req.GetInstanceId()]
	if ok {
		delete(s.instances, req.GetInstanceId())
	}
	s.mu.Unlock()
	if !ok {
		return &runtimepb.CloseInstanceResponse{
			Error: runtimev1.ErrorToProto(fmt.Errorf("unknown instance_id %d", req.GetInstanceId())),
		}, nil
	}
	if inst.processor != nil {
		if err := inst.processor.Close(ctx); err != nil {
			return &runtimepb.CloseInstanceResponse{Error: runtimev1.ErrorToProto(err)}, nil
		}
	}
	return &runtimepb.CloseInstanceResponse{}, nil
}

// ProcessBatch dispatches a message batch to the named processor
// instance.
func (s *server) ProcessBatch(ctx context.Context, req *runtimepb.InstanceProcessRequest) (*runtimepb.InstanceProcessResponse, error) {
	s.mu.RLock()
	inst, ok := s.instances[req.GetInstanceId()]
	s.mu.RUnlock()
	if !ok || inst.processor == nil {
		return &runtimepb.InstanceProcessResponse{
			Error: runtimev1.ErrorToProto(fmt.Errorf("unknown processor instance_id %d", req.GetInstanceId())),
		}, nil
	}

	batch, err := runtimev1.ProtoToMessageBatch(req.GetBatch())
	if err != nil {
		return &runtimepb.InstanceProcessResponse{
			Error: runtimev1.ErrorToProto(fmt.Errorf("decoding batch: %w", err)),
		}, nil
	}

	outBatches, err := inst.processor.ProcessBatch(ctx, batch)
	if err != nil {
		return &runtimepb.InstanceProcessResponse{Error: runtimev1.ErrorToProto(err)}, nil
	}

	pbBatches := make([]*runtimev1.MessageBatch, 0, len(outBatches))
	for _, b := range outBatches {
		pb, err := runtimev1.MessageBatchToProto(b)
		if err != nil {
			return &runtimepb.InstanceProcessResponse{
				Error: runtimev1.ErrorToProto(fmt.Errorf("encoding batch: %w", err)),
			}, nil
		}
		pbBatches = append(pbBatches, pb)
	}
	return &runtimepb.InstanceProcessResponse{Batches: pbBatches}, nil
}

func (s *server) lookupProcessor(name string) *registeredProcessor {
	for i := range s.env.procs {
		if s.env.procs[i].name == name {
			return &s.env.procs[i]
		}
	}
	return nil
}

// valueToYAML converts a wire-form Value to a YAML/JSON string suitable
// for *service.ConfigSpec.ParseYAML. JSON is a subset of YAML, so we
// can route through json.Marshal directly.
func valueToYAML(v *runtimev1.Value) (string, error) {
	if v == nil {
		return "{}", nil
	}
	val, err := runtimev1.ValueToAny(v)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "{}", nil
	}
	data, err := json.Marshal(val)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Logger is a trivial helper plugin authors can use to write structured
// logs to stderr. The host streams stderr through into its own log
// aggregator. For PoC purposes this is a thin wrapper around log.Printf.
func Logger() *log.Logger {
	return log.New(os.Stderr, "[plugin] ", log.LstdFlags|log.Lmicroseconds)
}
