// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package otlp

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "buf.build/gen/go/redpandadata/otel/protocolbuffers/go/redpanda/otel/v1"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/utils/netutil"
	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/connect/v4/internal/gateway"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp/otlpconv"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	giFieldAddress        = "address"
	giFieldTLS            = "tls"
	giFieldAuthToken      = "auth_token"
	giFieldMaxRecvMsgSize = "max_recv_msg_size"

	defaultGRPCAddress    = "0.0.0.0:4317"
	defaultMaxRecvMsgSize = 4 * 1024 * 1024 // 4MB

	otlpGRPCPermission authz.PermissionName = "dataplane_pipeline_otlp_grpc_invoke"
)

type grpcInputConfig struct {
	Address        string
	TLS            tlsServerConfig
	AuthToken      string
	MaxRecvMsgSize int
	ListenerConfig netutil.ListenerConfig
}

// GRPCInputSpec returns the configuration spec for the OTLP gRPC input.
func GRPCInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network", "Services").
		Version("4.78.0").
		Summary("Receive OpenTelemetry traces, logs, and metrics via OTLP/gRPC protocol.").
		Description(`
Exposes an OpenTelemetry Collector gRPC receiver that accepts traces, logs, and metrics via gRPC.

Telemetry data is received in OTLP protobuf format and converted to individual Redpanda OTEL v1 messages.
Each signal (span, log record, or metric) becomes a separate message with embedded Resource and Scope metadata.

## Protocols

This input supports OTLP/gRPC on the default port 4317 using the standard OTLP protobuf format for all signal types (traces, logs, metrics).

## Output Format

Each OTLP export request is unbatched into individual messages:
- **Traces**: One message per span
- **Logs**: One message per log record
- **Metrics**: One message per metric

Messages are encoded in Redpanda OTEL v1 format (protobuf or JSON, configurable via `+"`encoding`"+` field).

Each message includes the following metadata:
- `+"`otel_signal_type`"+`: The signal type - "trace", "log", or "metric"
- `+"`otel_encoding`"+` : The message encoding - "json" or "protobuf"

## Authentication

When `+"`auth_token`"+` is configured, clients must include the token in the gRPC metadata:

**Go Client Example:**
`+"```go"+`
import (
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

exporter, err := otlptracegrpc.New(ctx,
    otlptracegrpc.WithEndpoint("localhost:4317"),
    otlptracegrpc.WithInsecure(), // or WithTLSCredentials() for TLS
    otlptracegrpc.WithHeaders(map[string]string{
        "authorization": "Bearer your-token-here",
    }),
)
`+"```"+`

**Environment Variable:**
`+"```bash"+`
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer your-token-here"
`+"```"+`

## Rate Limiting

An optional rate limit resource can be specified to throttle incoming requests. When the rate limit is breached, requests will receive a ResourceExhausted gRPC status code.
`).
		Fields(
			service.NewStringEnumField(fieldEncoding, "protobuf", "json").
				Description("Encoding format for messages in the batch. Options: 'protobuf' or 'json'.").
				Default(string(EncodingJSON)),
			service.NewStringField(giFieldAddress).
				Description("The address to listen on for gRPC connections.").
				Default(defaultGRPCAddress),
			service.NewObjectField(giFieldTLS,
				tlsServerConfigFields()...,
			).Description("TLS configuration for gRPC.").
				Advanced(),
			service.NewStringField(giFieldAuthToken).
				Description("Optional bearer token for authentication. When set, requests must include 'authorization: Bearer <token>' metadata.").
				Default("").
				Secret().
				Advanced(),
			service.NewIntField(giFieldMaxRecvMsgSize).
				Description("Maximum size of gRPC messages to receive in bytes.").
				Default(defaultMaxRecvMsgSize).
				Advanced(),
			service.NewStringField(fieldRateLimit).
				Description("An optional rate limit resource to throttle requests.").
				Default(""),
			netutil.ListenerConfigSpec(),
			service.NewObjectField(schemaRegistryField, schemaRegistryConfigFields()...).
				Description("Optional Schema Registry configuration for adding Schema Registry wire format headers to messages.").
				Optional().
				Advanced(),
		)
}

//------------------------------------------------------------------------------

type grpcOTLPInput struct {
	otlpInput
	conf        grpcInputConfig
	authzPolicy *gateway.FileWatchingAuthzResourcePolicy
	rpJWT       *gateway.RPGRPCJWTInterceptor
	server      *grpc.Server
	done        chan struct{}
}

// GRPCInputFromParsed creates an OTLP gRPC input from a parsed config.
func GRPCInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	var (
		conf grpcInputConfig
		err  error
	)

	// Parse gRPC-specific config
	if conf.Address, err = pConf.FieldString(giFieldAddress); err != nil {
		return nil, err
	}
	if conf.MaxRecvMsgSize, err = pConf.FieldInt(giFieldMaxRecvMsgSize); err != nil {
		return nil, err
	}

	// Parse TLS config
	if pConf.Contains(giFieldTLS) {
		if conf.TLS, err = parseTLSServerConfig(pConf.Namespace(giFieldTLS)); err != nil {
			return nil, err
		}
	}

	// Parse auth token
	if conf.AuthToken, err = pConf.FieldString(giFieldAuthToken); err != nil {
		return nil, err
	}

	// Parse netutil listener config
	if conf.ListenerConfig, err = netutil.ListenerConfigFromParsed(pConf.Namespace("tcp")); err != nil {
		return nil, fmt.Errorf("parse tcp config: %w", err)
	}

	// Initialize authorization policy if configured
	var authzPolicy *gateway.FileWatchingAuthzResourcePolicy
	if authzConf, ok := gateway.ManagerAuthzConfig(mgr); ok {
		authzPolicy, err = gateway.NewFileWatchingAuthzResourcePolicy(
			authzConf.ResourceName,
			authzConf.PolicyFile,
			[]authz.PermissionName{otlpGRPCPermission},
			func(err error) {
				mgr.Logger().With("error", err).Error("Authorization policy error")
			},
		)
		if err != nil {
			return nil, fmt.Errorf("initialize authorization policy: %w", err)
		}
	}

	// Initialize JWT interceptor
	rpJWT, err := gateway.NewRPGRPCJWTInterceptor(mgr)
	if err != nil {
		return nil, err
	}

	otlpIn, err := newOTLPInputFromParsed(pConf, mgr)
	if err != nil {
		return nil, err
	}
	return &grpcOTLPInput{
		otlpInput:   otlpIn,
		conf:        conf,
		authzPolicy: authzPolicy,
		rpJWT:       rpJWT,
		done:        make(chan struct{}),
	}, nil
}

func init() {
	service.MustRegisterBatchInput("otlp_grpc", GRPCInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		return GRPCInputFromParsed(conf, mgr)
	})
}

//------------------------------------------------------------------------------

// Connect starts the gRPC server.
func (gi *grpcOTLPInput) Connect(ctx context.Context) error {
	if gi.server != nil {
		return nil
	}

	// Initialize Schema Registry
	if err := gi.maybeInitSchemaRegistry(ctx); err != nil {
		return fmt.Errorf("initialize schema registry: %w", err)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(gi.conf.MaxRecvMsgSize),
	}
	if gi.conf.TLS.Enabled {
		cert, err := tls.LoadX509KeyPair(gi.conf.TLS.CertFile, gi.conf.TLS.KeyFile)
		if err != nil {
			return fmt.Errorf("load TLS certificate: %w", err)
		}
		creds := credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
		opts = append(opts, grpc.Creds(creds))
	}

	// Build interceptor chain: JWT -> Authz
	var (
		unaryInterceptors  []grpc.UnaryServerInterceptor
		streamInterceptors []grpc.StreamServerInterceptor
	)

	if gi.rpJWT != nil {
		unaryInterceptors = append(unaryInterceptors, gi.rpJWT.UnaryInterceptor())
		streamInterceptors = append(streamInterceptors, gi.rpJWT.StreamInterceptor())
	}

	if gi.authzPolicy != nil {
		if gi.rpJWT == nil {
			return errors.New("authorization policy requires JWT authentication to be enabled")
		}

		unaryInterceptors = append(unaryInterceptors, gateway.GRPCUnaryAuthzInterceptor(gi.authzPolicy, otlpGRPCPermission))
		streamInterceptors = append(streamInterceptors, gateway.GRPCStreamAuthzInterceptor(gi.authzPolicy, otlpGRPCPermission))
	}

	if len(unaryInterceptors) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	}
	if len(streamInterceptors) > 0 {
		opts = append(opts, grpc.ChainStreamInterceptor(streamInterceptors...))
	}

	gi.server = grpc.NewServer(opts...)

	// Register services
	ptraceotlp.RegisterGRPCServer(gi.server, newTraceServiceServer(gi))
	plogotlp.RegisterGRPCServer(gi.server, newLogsServiceServer(gi))
	pmetricotlp.RegisterGRPCServer(gi.server, newMetricsServiceServer(gi))

	// Create listener
	var lc net.ListenConfig
	if err := netutil.DecorateListenerConfig(&lc, gi.conf.ListenerConfig); err != nil {
		return fmt.Errorf("configure listener: %w", err)
	}
	ln, err := lc.Listen(ctx, "tcp", gi.conf.Address)
	if err != nil {
		return fmt.Errorf("create gRPC listener: %w", err)
	}

	gi.log.Infof("Starting OTLP gRPC server on %s", gi.conf.Address)
	go func() {
		if serr := gi.server.Serve(ln); serr != nil && !errors.Is(serr, grpc.ErrServerStopped) {
			gi.log.Errorf("gRPC server error: %v", serr)
		}
		close(gi.done)
	}()

	return nil
}

const gracefulShutdownTimeout = 5 * time.Second

// Close shuts down the gRPC server.
func (gi *grpcOTLPInput) Close(ctx context.Context) error {
	gi.shutSig.TriggerSoftStop()
	defer gi.shutSig.TriggerHasStopped()

	if gi.server == nil {
		return gi.authzPolicy.Close()
	}

	// Shutdown gRPC server gracefully
	go func() {
		gi.server.GracefulStop()
	}()

	select {
	case <-gi.done:
		gi.log.Info("OTLP gRPC input shut down successfully")
	case <-time.After(gracefulShutdownTimeout):
		gi.log.Debug("OTLP gRPC input graceful shutdown timed out, forcing shutdown")
		gi.server.Stop()
	case <-ctx.Done():
		gi.log.Warn("OTLP gRPC input shutdown timed out")
		gi.server.Stop()
	}

	return gi.authzPolicy.Close()
}

// validateAuth checks the authorization header in the gRPC metadata
func (gi *grpcOTLPInput) validateAuth(ctx context.Context) error {
	if gi.conf.AuthToken == "" {
		return nil // No auth configured
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := authHeaders[0]
	expectedAuth := "Bearer " + gi.conf.AuthToken

	if subtle.ConstantTimeCompare([]byte(authHeader), []byte(expectedAuth)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid authorization token")
	}

	return nil
}

// traceServiceServer implements the gRPC trace service.
type traceServiceServer struct {
	ptraceotlp.UnimplementedGRPCServer
	*grpcOTLPInput
}

func newTraceServiceServer(gi *grpcOTLPInput) *traceServiceServer {
	return &traceServiceServer{
		grpcOTLPInput: gi,
	}
}

// Export implements the gRPC Export method for traces
func (s *traceServiceServer) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		s.log.Warnf("Authentication failed: %s", err)
		return ptraceotlp.NewExportResponse(), err
	}

	s.maybeWaitForAccess(ctx)

	if req.Traces().SpanCount() == 0 {
		return ptraceotlp.NewExportResponse(), nil
	}

	batch := make(service.MessageBatch, 0, otlpconv.SpansCount(req))
	var marshalErr error
	otlpconv.TracesToRedpandaFunc(req, func(span *pb.Span) bool {
		msg, err := s.newMessageWithSignalType(span, SignalTypeTrace)
		if err != nil {
			marshalErr = err
			return false
		}

		batch = append(batch, msg)
		return true
	})

	if marshalErr != nil {
		s.log.Warnf("Failed to marshal span: %v", marshalErr)
		return ptraceotlp.NewExportResponse(), status.Error(codes.Internal, "failed to marshal span")
	}

	resCh, err := s.sendMessageBatch(ctx, batch)
	if err != nil {
		if errors.Is(err, service.ErrNotConnected) {
			return ptraceotlp.NewExportResponse(), status.Error(codes.Unavailable, "server closing")
		}
		return ptraceotlp.NewExportResponse(), status.Error(codes.Unavailable, "request timeout")
	}

	select {
	case err := <-resCh:
		if err != nil {
			return ptraceotlp.NewExportResponse(), status.Error(codes.Internal, err.Error())
		}
	case <-ctx.Done():
		return ptraceotlp.NewExportResponse(), status.Error(codes.Unavailable, "request timeout")
	case <-s.shutSig.SoftStopChan():
		return ptraceotlp.NewExportResponse(), status.Error(codes.Unavailable, "server closing")
	}

	return ptraceotlp.NewExportResponse(), nil
}

// logsServiceServer implements the gRPC logs service.
type logsServiceServer struct {
	plogotlp.UnimplementedGRPCServer
	*grpcOTLPInput
}

func newLogsServiceServer(gi *grpcOTLPInput) *logsServiceServer {
	return &logsServiceServer{
		grpcOTLPInput: gi,
	}
}

func (s *logsServiceServer) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return plogotlp.NewExportResponse(), err
	}

	s.maybeWaitForAccess(ctx)

	logs := req.Logs()
	if logs.LogRecordCount() == 0 {
		return plogotlp.NewExportResponse(), nil
	}

	batch := make(service.MessageBatch, 0, otlpconv.LogsCount(req))
	var marshalErr error
	otlpconv.LogsToRedpandaFunc(req, func(logRecord *pb.LogRecord) bool {
		msg, err := s.newMessageWithSignalType(logRecord, SignalTypeLog)
		if err != nil {
			marshalErr = err
			return false
		}

		batch = append(batch, msg)
		return true
	})

	if marshalErr != nil {
		s.log.Warnf("Failed to marshal log record: %v", marshalErr)
		return plogotlp.NewExportResponse(), status.Error(codes.Internal, "failed to marshal log record")
	}

	// Send batch
	resCh, err := s.sendMessageBatch(ctx, batch)
	if err != nil {
		if errors.Is(err, service.ErrNotConnected) {
			return plogotlp.NewExportResponse(), status.Error(codes.Unavailable, "server closing")
		}
		return plogotlp.NewExportResponse(), status.Error(codes.Unavailable, "request timeout")
	}

	select {
	case err := <-resCh:
		if err != nil {
			return plogotlp.NewExportResponse(), status.Error(codes.Internal, err.Error())
		}
	case <-ctx.Done():
		return plogotlp.NewExportResponse(), status.Error(codes.Unavailable, "request timeout")
	case <-s.shutSig.SoftStopChan():
		return plogotlp.NewExportResponse(), status.Error(codes.Unavailable, "server closing")
	}

	return plogotlp.NewExportResponse(), nil
}

// metricsServiceServer implements the gRPC metrics service.
type metricsServiceServer struct {
	pmetricotlp.UnimplementedGRPCServer
	*grpcOTLPInput
}

func newMetricsServiceServer(gi *grpcOTLPInput) *metricsServiceServer {
	return &metricsServiceServer{
		grpcOTLPInput: gi,
	}
}

// Export implements the gRPC Export method for metrics
func (s *metricsServiceServer) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return pmetricotlp.NewExportResponse(), err
	}

	s.maybeWaitForAccess(ctx)

	metrics := req.Metrics()
	if metrics.DataPointCount() == 0 {
		return pmetricotlp.NewExportResponse(), nil
	}

	batch := make(service.MessageBatch, 0, otlpconv.MetricsCount(req))
	var marshalErr error
	otlpconv.MetricsToRedpandaFunc(req, func(metric *pb.Metric) bool {
		msg, err := s.newMessageWithSignalType(metric, SignalTypeMetric)
		if err != nil {
			marshalErr = err
			return false
		}

		batch = append(batch, msg)
		return true
	})

	if marshalErr != nil {
		s.log.Warnf("Failed to marshal metric: %v", marshalErr)
		return pmetricotlp.NewExportResponse(), status.Error(codes.Internal, "failed to marshal metric")
	}

	// Send batch
	resCh, err := s.sendMessageBatch(ctx, batch)
	if err != nil {
		if errors.Is(err, service.ErrNotConnected) {
			return pmetricotlp.NewExportResponse(), status.Error(codes.Unavailable, "server closing")
		}
		return pmetricotlp.NewExportResponse(), status.Error(codes.Unavailable, "request timeout")
	}

	select {
	case err := <-resCh:
		if err != nil {
			return pmetricotlp.NewExportResponse(), status.Error(codes.Internal, err.Error())
		}
	case <-ctx.Done():
		return pmetricotlp.NewExportResponse(), status.Error(codes.Unavailable, "request timeout")
	case <-s.shutSig.SoftStopChan():
		return pmetricotlp.NewExportResponse(), status.Error(codes.Unavailable, "server closing")
	}

	return pmetricotlp.NewExportResponse(), nil
}
