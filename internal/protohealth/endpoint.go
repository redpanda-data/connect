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

package protohealth

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Endpoint hosts a grpc health endpoint at the specified port.
// No TLS is wrapped around this; it's for k8s consumption.
type Endpoint struct {
	port    int16
	srv     *grpc.Server
	running atomic.Bool
	signal  chan struct{}
	grpc_health_v1.UnimplementedHealthServer
}

// NewEndpoint constructs the Endpoint
func NewEndpoint(port int16) *Endpoint {
	srv := grpc.NewServer()
	reflection.Register(srv)
	e := &Endpoint{
		port:   port,
		srv:    srv,
		signal: make(chan struct{}),
	}
	grpc_health_v1.RegisterHealthServer(srv, e)

	return e
}

// Run listens on the supplied GRPC health endpoint for unencrypted connections
func (e *Endpoint) Run(ctx context.Context) error {
	e.running.Store(true)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", e.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	errC := make(chan error, 1)
	go func() {
		errC <- e.srv.Serve(lis)
	}()
	select {
	case <-ctx.Done():
		e.srv.Stop()
		return ctx.Err()
	case err := <-errC:
		return err
	}
}

// MarkDone should be called to latch the Endpoint into "not ready"
// status. This cannot be reversed. All watchers will be notified.
func (e *Endpoint) MarkDone() {
	if e.running.Swap(false) {
		close(e.signal)
	}
}

// Check is the one-shot GRPC test endpoint.
func (e *Endpoint) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	status := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if e.running.Load() {
		status = grpc_health_v1.HealthCheckResponse_SERVING
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: status,
	}, nil
}

// Watch is the streaming GRPC endpoint.
func (e *Endpoint) Watch(_ *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	status := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if e.running.Load() {
		status = grpc_health_v1.HealthCheckResponse_SERVING
	}

	err := server.Send(&grpc_health_v1.HealthCheckResponse{
		Status: status,
	})
	if err != nil {
		return err
	}

	watcher := e.signal
	for {
		select {
		case <-server.Context().Done():
			return server.Context().Err()
		case <-watcher:
			watcher = nil
			err := server.Send(&grpc_health_v1.HealthCheckResponse{
				Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
			})
			if err != nil {
				return err
			}
		}
	}
}
