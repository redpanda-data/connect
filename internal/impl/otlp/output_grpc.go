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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/utils/netutil"
	"github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp/otlpconv"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/oauth2"
)

const (
	goFieldEndpoint    = "endpoint"
	goFieldHeaders     = "headers"
	goFieldTimeout     = "timeout"
	goFieldCompression = "compression"
	goFieldTLS         = "tls"

	defaultTimeout     = 30 * time.Second
	defaultCompression = "gzip"
)

type grpcOutputConfig struct {
	Endpoint     string
	Headers      map[string]*service.InterpolatedString
	TLS          tlsClientConfig
	OAuth2       oauth2.Config
	Timeout      time.Duration
	Compression  string
	DialerConfig netutil.DialerConfig
}

// GRPCOutputSpec returns the configuration spec for the OTLP gRPC output.
func GRPCOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Version("4.78.0").
		Summary("Send OpenTelemetry traces, logs, and metrics via OTLP/gRPC protocol.").
		Description(`
Sends OpenTelemetry telemetry data to a remote collector via OTLP/gRPC protocol.

Accepts batches of Redpanda OTEL v1 protobuf messages (spans, log records, or metrics) and converts them to OTLP format for transmission to OpenTelemetry collectors.

## Input Format

Expects messages in Redpanda OTEL v1 protobuf format with metadata:
- `+"`signal_type`"+`: "trace", "log", or "metric"

Each batch must contain messages of the same signal type.
The entire batch is converted to a single OTLP export request and sent via gRPC.

## Authentication

Supports multiple authentication methods:
- Bearer token authentication (via auth_token field)
- OAuth v2 (via oauth2 configuration block)

Note: OAuth2 requires TLS to be enabled.
`).
		Fields(
			service.NewStringField(goFieldEndpoint).
				Description("The gRPC endpoint of the remote OTLP collector."),
			service.NewInterpolatedStringMapField(goFieldHeaders).
				Description("A map of headers to add to the gRPC request metadata.").
				Example(map[string]any{
					"X-Custom-Header": "value",
					"traceparent":     `${! tracing_span().traceparent }`,
				}).
				Default(map[string]any{}).
				Advanced(),
			service.NewDurationField(goFieldTimeout).
				Description("Timeout for gRPC requests.").
				Default("30s").
				Advanced(),
			service.NewStringEnumField(goFieldCompression, "gzip", "none").
				Description("Compression type for gRPC requests. Options: 'gzip' or 'none'.").
				Default(defaultCompression).
				Advanced(),
			service.NewObjectField(goFieldTLS,
				tlsClientConfigFields()...,
			).Description("TLS configuration for gRPC client.").
				Advanced().
				Optional(),
			netutil.DialerConfigSpec(),
		).
		Fields(oauth2.FieldSpec()).
		Fields(service.NewOutputMaxInFlightField())
}

//------------------------------------------------------------------------------

type grpcOTLPOutput struct {
	otlpOutput

	conf grpcOutputConfig

	conn         *grpc.ClientConn
	traceClient  ptraceotlp.GRPCClient
	logClient    plogotlp.GRPCClient
	metricClient pmetricotlp.GRPCClient
}

// GRPCOutputFromParsed creates an OTLP gRPC output from a parsed config.
func GRPCOutputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	var (
		conf grpcOutputConfig
		err  error
	)

	// Parse gRPC-specific config
	if conf.Endpoint, err = pConf.FieldString(goFieldEndpoint); err != nil {
		return nil, err
	}
	if conf.Headers, err = pConf.FieldInterpolatedStringMap(goFieldHeaders); err != nil {
		return nil, err
	}
	if conf.Timeout, err = pConf.FieldDuration(goFieldTimeout); err != nil {
		return nil, err
	}
	if conf.Compression, err = pConf.FieldString(goFieldCompression); err != nil {
		return nil, err
	}

	// Parse TLS config
	if pConf.Contains(goFieldTLS) {
		if conf.TLS, err = parseTLSClientConfig(pConf.Namespace(goFieldTLS)); err != nil {
			return nil, err
		}
	}

	// Parse OAuth2 config
	if pConf.Contains("oauth2") {
		if conf.OAuth2, err = oauth2.ParseConfig(pConf.Namespace("oauth2")); err != nil {
			return nil, fmt.Errorf("parse oauth2 config: %w", err)
		}
		if conf.OAuth2.Enabled && !conf.TLS.Enabled {
			return nil, errors.New("oauth2 requires TLS to be enabled")
		}
	}

	// Parse netutil dialer config
	if pConf.Contains("tcp") {
		if conf.DialerConfig, err = netutil.DialerConfigFromParsed(pConf.Namespace("tcp")); err != nil {
			return nil, fmt.Errorf("parse tcp config: %w", err)
		}
	}

	return &grpcOTLPOutput{
		otlpOutput: newOTLPOutput(mgr),
		conf:       conf,
	}, nil
}

func init() {
	service.MustRegisterBatchOutput("otlp_grpc", GRPCOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			o service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if o, err = GRPCOutputFromParsed(conf, mgr); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}

			return
		})
}

//------------------------------------------------------------------------------

// Connect establishes the gRPC connection and initializes clients.
func (o *grpcOTLPOutput) Connect(_ context.Context) error {
	if o.conn != nil {
		return nil
	}

	var opts []grpc.DialOption

	// Configure custom dialer with TCP options
	var nd net.Dialer
	if err := netutil.DecorateDialer(&nd, o.conf.DialerConfig); err != nil {
		return fmt.Errorf("configure custom dialer: %w", err)
	}
	opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		return nd.DialContext(ctx, "tcp", addr)
	}))

	// Configure TLS
	if o.conf.TLS.Enabled {
		tlsConf := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: o.conf.TLS.SkipCertVerify,
		}

		// Load client certificate if provided
		if o.conf.TLS.CertFile != "" && o.conf.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(o.conf.TLS.CertFile, o.conf.TLS.KeyFile)
			if err != nil {
				return fmt.Errorf("load TLS certificate: %w", err)
			}
			tlsConf.Certificates = []tls.Certificate{cert}
		}

		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Configure compression
	if o.conf.Compression == "gzip" {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}

	// Configure OAuth2 if enabled
	if o.conf.OAuth2.Enabled {
		ctx, _ := o.shutSig.SoftStopCtx(context.Background())
		opts = append(opts, grpc.WithPerRPCCredentials(
			oauth.TokenSource{TokenSource: o.conf.OAuth2.TokenSource(ctx)}))
	}

	// Establish connection
	conn, err := grpc.NewClient(o.conf.Endpoint, opts...)
	if err != nil {
		return fmt.Errorf("create gRPC client: %w", err)
	}

	o.conn = conn
	o.traceClient = ptraceotlp.NewGRPCClient(conn)
	o.logClient = plogotlp.NewGRPCClient(conn)
	o.metricClient = pmetricotlp.NewGRPCClient(conn)

	o.log.Infof("Connected to OTLP gRPC endpoint: %s", o.conf.Endpoint)
	return nil
}

// WriteBatch converts and sends a batch of messages to the remote collector.
func (o *grpcOTLPOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	// Apply timeout
	if o.conf.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.conf.Timeout)
		defer cancel()
	}

	// Detect signal type from first message
	signalType, err := detectSignalType(batch)
	if err != nil {
		return fmt.Errorf("detect signal type: %w", err)
	}

	// Convert and send based on signal type
	switch signalType {
	case SignalTypeTrace:
		return o.sendTraces(ctx, batch)
	case SignalTypeLog:
		return o.sendLogs(ctx, batch)
	case SignalTypeMetric:
		return o.sendMetrics(ctx, batch)
	default:
		return fmt.Errorf("unknown signal_type: %s", signalType)
	}
}

func (o *grpcOTLPOutput) headersFrom(ctx context.Context, batch service.MessageBatch) (context.Context, error) {
	if len(o.conf.Headers) == 0 {
		return ctx, nil
	}

	md := metadata.New(nil)
	for k, v := range o.conf.Headers {
		hv, err := batch.TryInterpolatedString(0, v)
		if err != nil {
			return nil, fmt.Errorf("header '%s' interpolation error: %w", k, err)
		}
		md.Append(k, hv)
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}

func (o *grpcOTLPOutput) sendTraces(ctx context.Context, batch service.MessageBatch) error {
	spans, err := unmarshalBatch[proto.Span](batch, "span")
	if err != nil {
		return fmt.Errorf("unmarshal spans: %w", err)
	}

	ctx, err = o.headersFrom(ctx, batch)
	if err != nil {
		return fmt.Errorf("headers: %w", err)
	}

	req := otlpconv.TracesFromRedpanda(spans)
	resp, err := o.traceClient.Export(ctx, req)
	if err != nil {
		return fmt.Errorf("export traces: %w", err)
	}
	if s := resp.PartialSuccess(); s.RejectedSpans() > 0 {
		return fmt.Errorf("export traces: %d spans were rejected by the collector: %s",
			s.RejectedSpans(), s.ErrorMessage())
	}

	return nil
}

func (o *grpcOTLPOutput) sendLogs(ctx context.Context, batch service.MessageBatch) error {
	logs, err := unmarshalBatch[proto.LogRecord](batch, "log record")
	if err != nil {
		return fmt.Errorf("unmarshal logs: %w", err)
	}

	ctx, err = o.headersFrom(ctx, batch)
	if err != nil {
		return fmt.Errorf("headers: %w", err)
	}

	req := otlpconv.LogsFromRedpanda(logs)
	resp, err := o.logClient.Export(ctx, req)
	if err != nil {
		return fmt.Errorf("export logs: %w", err)
	}
	if s := resp.PartialSuccess(); s.RejectedLogRecords() > 0 {
		return fmt.Errorf("export logs: %d spans were rejected by the collector: %s",
			s.RejectedLogRecords(), s.ErrorMessage())
	}

	return nil
}

func (o *grpcOTLPOutput) sendMetrics(ctx context.Context, batch service.MessageBatch) error {
	metrics, err := unmarshalBatch[proto.Metric](batch, "metric")
	if err != nil {
		return fmt.Errorf("unmarshal metrics: %w", err)
	}

	ctx, err = o.headersFrom(ctx, batch)
	if err != nil {
		return fmt.Errorf("headers: %w", err)
	}

	req := otlpconv.MetricsFromRedpanda(metrics)
	resp, err := o.metricClient.Export(ctx, req)
	if err != nil {
		return fmt.Errorf("export metrics: %w", err)
	}
	if s := resp.PartialSuccess(); s.RejectedDataPoints() > 0 {
		return fmt.Errorf("export metrics: %d spans were rejected by the collector: %s",
			s.RejectedDataPoints(), s.ErrorMessage())
	}

	return nil
}

// Close closes the gRPC connection.
func (o *grpcOTLPOutput) Close(_ context.Context) error {
	o.shutSig.TriggerSoftStop()
	defer o.shutSig.TriggerHasStopped()

	if o.conn == nil {
		return nil
	}

	if err := o.conn.Close(); err != nil {
		return fmt.Errorf("close gRPC connection: %w", err)
	}

	return nil
}
