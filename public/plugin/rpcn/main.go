// Copyright 2025 Redpanda Data, Inc.
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

// Package rpcplugin contains a library supporting writing plugins that are run dynamically over gRPC
// instead of having to compile in support for a new component.
package rpcn

// !!! NOTE !!!
// If you're looking at the source of this package to reimplment it for your language than please open
// an issue at github.com/redpanda-data/connect and let us know. We would love to help you out, and it's
// likely we're going to move quickly here and change versions and you're going to be way better off
// working with us instead of trying to keep up with changes here. And if you're willing to write an SDK
// then the whole community will benefit. Win, win right?
// !!! NOTE !!!

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb"
	"google.golang.org/grpc"
)

// ProcessorConstructor is the factory function to create a new batch processor.
type ProcessorConstructor[T any] func(config T) (processor service.BatchProcessor, err error)

type processor struct {
	runtimepb.UnimplementedBatchProcessorServiceServer

	ctor      ProcessorConstructor[any]
	component service.BatchProcessor
}

// Init implements runtimepb.BatchProcessorServiceServer.
func (p *processor) Init(ctx context.Context, req *runtimepb.BatchProcessorInitRequest) (*runtimepb.BatchProcessorInitResponse, error) {
	if p.component != nil {
		return &runtimepb.BatchProcessorInitResponse{Error: nil}, nil
	}
	config, err := runtimepb.ValueToAny(req.Config)
	if err != nil {
		return &runtimepb.BatchProcessorInitResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	component, err := p.ctor(config)
	if err != nil {
		return &runtimepb.BatchProcessorInitResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	p.component = component
	return &runtimepb.BatchProcessorInitResponse{Error: nil}, nil
}

// ProcessBatch implements runtimepb.BatchProcessorServiceServer.
func (p *processor) ProcessBatch(ctx context.Context, req *runtimepb.BatchProcessorProcessBatchRequest) (*runtimepb.BatchProcessorProcessBatchResponse, error) {
	if p.component == nil {
		return &runtimepb.BatchProcessorProcessBatchResponse{Error: runtimepb.ErrorToProto(service.ErrNotConnected)}, nil
	}
	batch, err := runtimepb.ProtoToMessageBatch(req.Batch)
	if err != nil {
		return &runtimepb.BatchProcessorProcessBatchResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	batches, err := p.component.ProcessBatch(ctx, batch)
	if err != nil {
		return &runtimepb.BatchProcessorProcessBatchResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	protos := make([]*runtimepb.MessageBatch, 0, len(batches))
	for _, batch := range batches {
		proto, err := runtimepb.MessageBatchToProto(batch)
		if err != nil {
			return &runtimepb.BatchProcessorProcessBatchResponse{Error: runtimepb.ErrorToProto(err)}, nil
		}
		protos = append(protos, proto)
	}
	return &runtimepb.BatchProcessorProcessBatchResponse{Batches: protos}, nil
}

// Close implements runtimepb.BatchProcessorServiceServer.
func (p *processor) Close(ctx context.Context, req *runtimepb.BatchProcessorCloseRequest) (*runtimepb.BatchProcessorCloseResponse, error) {
	if p.component == nil {
		return &runtimepb.BatchProcessorCloseResponse{Error: nil}, nil
	}
	err := p.component.Close(ctx)
	return &runtimepb.BatchProcessorCloseResponse{Error: runtimepb.ErrorToProto(err)}, nil
}

// ProcessorMain should be called in your main function to initial the RPC plugin service and process messages.
// The configuration object given to the constructor is strongly typed, and deserialized using encoding/json rules.
func ProcessorMain[T any](ctor ProcessorConstructor[T]) {
	GenericProcessorMain(func(config any) (service.BatchProcessor, error) {
		typed, err := typedFromAny[T](config)
		if err != nil {
			return nil, err
		}
		return ctor(typed)
	})
}

// GenericProcessorMain is the same as ProcessorMain except that it does not give a strongly typed configuration object
func GenericProcessorMain(ctor ProcessorConstructor[any]) {
	runMain(func(s *grpc.Server) {
		runtimepb.RegisterBatchProcessorServiceServer(s, &processor{ctor: ctor})
	})
}

// OutputConstructor is the factory function to create a new batch output.
type OutputConstructor[T any] func(config T) (output service.BatchOutput, maxInFlight int, batchPolicy service.BatchPolicy, err error)

type output struct {
	runtimepb.UnimplementedBatchOutputServiceServer

	ctor      OutputConstructor[any]
	component service.BatchOutput
}

// Init implements runtimepb.BatchOutputServiceServer.
func (o *output) Init(ctx context.Context, req *runtimepb.BatchOutputInitRequest) (*runtimepb.BatchOutputInitResponse, error) {
	if o.component != nil {
		return &runtimepb.BatchOutputInitResponse{Error: nil}, nil
	}
	config, err := runtimepb.ValueToAny(req.Config)
	if err != nil {
		return &runtimepb.BatchOutputInitResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	component, maxInFlight, batchPolicy, err := o.ctor(config)
	if err != nil {
		return &runtimepb.BatchOutputInitResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	o.component = component
	return &runtimepb.BatchOutputInitResponse{
		Error:       nil,
		MaxInFlight: int32(maxInFlight),
		BatchPolicy: &runtimepb.BatchPolicy{
			ByteSize: int64(batchPolicy.ByteSize),
			Count:    int64(batchPolicy.Count),
			Period:   batchPolicy.Period,
			Check:    batchPolicy.Check,
		},
	}, nil
}

// Connect implements runtimepb.BatchOutputServiceServer.
func (o *output) Connect(ctx context.Context, req *runtimepb.BatchOutputConnectRequest) (*runtimepb.BatchOutputConnectResponse, error) {
	if o.component == nil {
		return &runtimepb.BatchOutputConnectResponse{Error: runtimepb.ErrorToProto(service.ErrNotConnected)}, nil
	}
	err := o.component.Connect(ctx)
	return &runtimepb.BatchOutputConnectResponse{Error: runtimepb.ErrorToProto(err)}, nil
}

// Send implements runtimepb.BatchOutputServiceServer.
func (o *output) Send(ctx context.Context, req *runtimepb.BatchOutputSendRequest) (*runtimepb.BatchOutputSendResponse, error) {
	if o.component == nil {
		return &runtimepb.BatchOutputSendResponse{Error: runtimepb.ErrorToProto(service.ErrNotConnected)}, nil
	}
	batch, err := runtimepb.ProtoToMessageBatch(req.Batch)
	if err != nil {
		return &runtimepb.BatchOutputSendResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	err = o.component.WriteBatch(ctx, batch)
	return &runtimepb.BatchOutputSendResponse{Error: runtimepb.ErrorToProto(err)}, nil
}

// Close implements runtimepb.BatchOutputServiceServer.
func (o *output) Close(ctx context.Context, req *runtimepb.BatchOutputCloseRequest) (*runtimepb.BatchOutputCloseResponse, error) {
	if o.component == nil {
		return &runtimepb.BatchOutputCloseResponse{Error: nil}, nil
	}
	err := o.component.Close(ctx)
	return &runtimepb.BatchOutputCloseResponse{Error: runtimepb.ErrorToProto(err)}, nil
}

// OutputMain should be called in your main function to initial the RPC plugin service and process messages.
// The configuration object given to the constructor is strongly typed, and deserialized using encoding/json rules.
func OutputMain[T any](ctor OutputConstructor[T]) {
	GenericOutputMain(func(config any) (service.BatchOutput, int, service.BatchPolicy, error) {
		typed, err := typedFromAny[T](config)
		if err != nil {
			return nil, 0, service.BatchPolicy{}, err
		}
		return ctor(typed)
	})
}

// GenericOutputMain is the same as OutputMain except that it does not give a strongly typed configuration object
func GenericOutputMain(ctor OutputConstructor[any]) {
	runMain(func(s *grpc.Server) {
		runtimepb.RegisterBatchOutputServiceServer(s, &output{ctor: ctor})
	})
}

// InputConstructor is the factory function to create a new batch input.
type InputConstructor[T any] func(config T) (output service.BatchInput, autoRetryNacks bool, err error)

type input struct {
	runtimepb.UnimplementedBatchInputServiceServer

	ctor             InputConstructor[any]
	component        service.BatchInput
	acks             sync.Map
	batchIDGenerator atomic.Uint64
}

// Init implements runtimepb.BatchInputServiceServer.
func (i *input) Init(ctx context.Context, req *runtimepb.BatchInputInitRequest) (*runtimepb.BatchInputInitResponse, error) {
	if i.component != nil {
		return &runtimepb.BatchInputInitResponse{Error: nil}, nil
	}
	config, err := runtimepb.ValueToAny(req.Config)
	if err != nil {
		return &runtimepb.BatchInputInitResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	component, autoRetryNacks, err := i.ctor(config)
	if err != nil {
		return &runtimepb.BatchInputInitResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	i.component = component
	return &runtimepb.BatchInputInitResponse{
		Error:           nil,
		AutoReplayNacks: autoRetryNacks,
	}, nil
}

// Connect implements runtimepb.BatchInputServiceServer.
func (i *input) Connect(ctx context.Context, req *runtimepb.BatchInputConnectRequest) (*runtimepb.BatchInputConnectResponse, error) {
	if i.component == nil {
		return &runtimepb.BatchInputConnectResponse{Error: runtimepb.ErrorToProto(service.ErrNotConnected)}, nil
	}
	err := i.component.Connect(ctx)
	return &runtimepb.BatchInputConnectResponse{Error: runtimepb.ErrorToProto(err)}, nil
}

// Close implements runtimepb.BatchInputServiceServer.
func (i *input) Close(ctx context.Context, req *runtimepb.BatchInputCloseRequest) (*runtimepb.BatchInputCloseResponse, error) {
	if i.component == nil {
		return &runtimepb.BatchInputCloseResponse{Error: nil}, nil
	}
	err := i.component.Close(ctx)
	return &runtimepb.BatchInputCloseResponse{Error: runtimepb.ErrorToProto(err)}, nil
}

// Ack implements runtimepb.BatchInputServiceServer.
func (i *input) Ack(ctx context.Context, req *runtimepb.BatchInputAckRequest) (*runtimepb.BatchInputAckResponse, error) {
	if i.component == nil {
		return &runtimepb.BatchInputAckResponse{Error: runtimepb.ErrorToProto(service.ErrNotConnected)}, nil
	}
	err := i.component.Close(ctx)
	return &runtimepb.BatchInputAckResponse{Error: runtimepb.ErrorToProto(err)}, nil
}

// ReadBatch implements runtimepb.BatchInputServiceServer.
func (i *input) ReadBatch(ctx context.Context, req *runtimepb.BatchInputReadRequest) (*runtimepb.BatchInputReadResponse, error) {
	if i.component == nil {
		return &runtimepb.BatchInputReadResponse{Error: runtimepb.ErrorToProto(service.ErrNotConnected)}, nil
	}
	batch, ack, err := i.component.ReadBatch(ctx)
	if err != nil {
		return &runtimepb.BatchInputReadResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	myID := i.batchIDGenerator.Add(1)
	i.acks.Store(myID, ack)
	proto, err := runtimepb.MessageBatchToProto(batch)
	if err != nil {
		return &runtimepb.BatchInputReadResponse{Error: runtimepb.ErrorToProto(err)}, nil
	}
	return &runtimepb.BatchInputReadResponse{BatchId: myID, Batch: proto}, nil
}

// InputMain should be called in your main function to initial the RPC plugin service and process messages.
// The configuration object given to the constructor is strongly typed, and deserialized using encoding/json rules.
func InputMain[T any](ctor InputConstructor[T]) {
	GenericInputMain(func(config any) (service.BatchInput, bool, error) {
		typed, err := typedFromAny[T](config)
		if err != nil {
			return nil, false, err
		}
		return ctor(typed)
	})
}

// GenericInputMain is the same as InputMain except that it does not give a strongly typed configuration object
func GenericInputMain(ctor InputConstructor[any]) {
	runMain(func(s *grpc.Server) {
		runtimepb.RegisterBatchInputServiceServer(s, &input{ctor: ctor})
	})
}

func typedFromAny[T any](v any) (result T, err error) {
	b, err := json.Marshal(v)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(b, &result); err != nil {
		return result, err
	}
	return result, nil
}

func runMain(register func(*grpc.Server)) {
	version, ok := os.LookupEnv("REDPANDA_CONNECT_PLUGIN_VERSION")
	if !ok {
		version = "1"
	}
	if version != "1" {
		log.Fatalf("unsupported REDPANDA_CONNECT_PLUGIN_VERSION: %s, supported versions: (1)", version)
	}
	addr, ok := os.LookupEnv("REDPANDA_CONNECT_PLUGIN_ADDRESS")
	if !ok {
		log.Fatal("REDPANDA_CONNECT_PLUGIN_ADDRESS not set")
	}
	fmt.Println("Successfully loaded Redpanda Connect RPC plugin")
	s := grpc.NewServer()
	register(s)
	var l net.Listener
	var err error
	switch {
	case strings.HasPrefix(addr, "unix://"):
		l, err = net.Listen("unix", strings.TrimPrefix(addr, "unix://"))
	case strings.HasPrefix(addr, "unix:"):
		l, err = net.Listen("unix", strings.TrimPrefix(addr, "unix:"))
	default:
		log.Fatalf("unknown REDPANDA_CONNECT_PLUGIN_ADDRESS scheme: %s", addr)
	}
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Handle shutdown gracefully
	shutdown := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down server...")
		s.GracefulStop()
		close(shutdown)
	}()

	if err := s.Serve(l); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	<-shutdown
	os.Exit(0)
}
