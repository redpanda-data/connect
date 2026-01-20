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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/utils/netutil"
	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
	"github.com/redpanda-data/connect/v4/internal/gateway"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp/otlpconv"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	hiFieldAddress      = "address"
	hiFieldTLS          = "tls"
	hiFieldAuthToken    = "auth_token"
	hiFieldReadTimeout  = "read_timeout"
	hiFieldWriteTimeout = "write_timeout"
	hiFieldMaxBodySize  = "max_body_size"

	defaultHTTPAddress      = "0.0.0.0:4318"
	defaultHTTPReadTimeout  = 10 * time.Second
	defaultHTTPWriteTimeout = 10 * time.Second
	defaultHTTPMaxBodySize  = 4 * 1024 * 1024 // 4MB
)

type httpInputConfig struct {
	Address        string
	TLS            tlsServerConfig
	AuthToken      string
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxBodySize    int
	ListenerConfig netutil.ListenerConfig
}

// HTTPInputSpec returns the configuration spec for the OTLP HTTP input.
func HTTPInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network", "Services").
		Version("4.78.0").
		Summary("Receive OpenTelemetry traces, logs, and metrics via OTLP/HTTP protocol.").
		Description(`
Exposes an OpenTelemetry Collector HTTP receiver that accepts traces, logs, and metrics via HTTP.

Telemetry data is received in OTLP format (protobuf or JSON) and converted to individual Redpanda OTEL v1 messages.
Each signal (span, log record, or metric) becomes a separate message with embedded Resource and Scope metadata.

## Endpoints

- `+"`/v1/traces`"+` - OpenTelemetry traces
- `+"`/v1/logs`"+` - OpenTelemetry logs
- `+"`/v1/metrics`"+` - OpenTelemetry metrics

## Protocols

This input supports OTLP/HTTP on the default port 4318. It accepts both:
- `+"`application/x-protobuf`"+` - OTLP protobuf format
- `+"`application/json`"+` - OTLP JSON format

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

When `+"`auth_token`"+` is configured, clients must include the token in the HTTP Authorization header:

**Go Client Example:**
`+"```go"+`
import (
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
)

exporter, err := otlptracehttp.New(ctx,
    otlptracehttp.WithEndpoint("localhost:4318"),
    otlptracehttp.WithInsecure(), // or WithTLSClientConfig() for TLS
    otlptracehttp.WithHeaders(map[string]string{
        "Authorization": "Bearer your-token-here",
    }),
)
`+"```"+`

**cURL Example:**
`+"```bash"+`
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/x-protobuf" \
  -H "Authorization: Bearer your-token-here" \
  --data-binary @traces.pb
`+"```"+`

**Environment Variable:**
`+"```bash"+`
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer your-token-here"
`+"```"+`

## Rate Limiting

An optional rate limit resource can be specified to throttle incoming requests. When the rate limit is breached, requests will receive a 429 (Too Many Requests) response.
`).
		Fields(
			service.NewStringEnumField(fieldEncoding, "protobuf", "json").
				Description("Encoding format for messages in the batch. Options: 'protobuf' or 'json'.").
				Default(string(EncodingJSON)),
			service.NewStringField(hiFieldAddress).
				Description("The address to listen on for HTTP connections.").
				Default(defaultHTTPAddress),
			service.NewObjectField(hiFieldTLS,
				tlsServerConfigFields()...,
			).Description("TLS configuration for HTTP.").
				Advanced(),
			service.NewStringField(hiFieldAuthToken).
				Description("Optional bearer token for authentication. When set, requests must include 'Authorization: Bearer <token>' header.").
				Default("").
				Secret().
				Advanced(),
			service.NewDurationField(hiFieldReadTimeout).
				Description("Maximum duration for reading the entire request.").
				Default(defaultHTTPReadTimeout.String()).
				Advanced(),
			service.NewDurationField(hiFieldWriteTimeout).
				Description("Maximum duration for writing the response.").
				Default(defaultHTTPWriteTimeout.String()).
				Advanced(),
			service.NewIntField(hiFieldMaxBodySize).
				Description("Maximum size of HTTP request body in bytes.").
				Default(defaultHTTPMaxBodySize).
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

// httpOTLPInput is the HTTP-specific OTLP input
type httpOTLPInput struct {
	otlpInput
	conf   httpInputConfig
	rpJWT  *gateway.RPJWTMiddleware
	cors   gateway.CORSConfig
	server *http.Server
}

// HTTPInputFromParsed creates an OTLP HTTP input from a parsed config.
func HTTPInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	var (
		conf httpInputConfig
		err  error
	)

	// Parse HTTP-specific config
	if conf.Address, err = pConf.FieldString(hiFieldAddress); err != nil {
		return nil, err
	}
	if conf.ReadTimeout, err = pConf.FieldDuration(hiFieldReadTimeout); err != nil {
		return nil, err
	}
	if conf.WriteTimeout, err = pConf.FieldDuration(hiFieldWriteTimeout); err != nil {
		return nil, err
	}
	if conf.MaxBodySize, err = pConf.FieldInt(hiFieldMaxBodySize); err != nil {
		return nil, err
	}

	// Parse TLS config
	if pConf.Contains(hiFieldTLS) {
		if conf.TLS, err = parseTLSServerConfig(pConf.Namespace(hiFieldTLS)); err != nil {
			return nil, err
		}
	}

	// Parse auth token
	if conf.AuthToken, err = pConf.FieldString(hiFieldAuthToken); err != nil {
		return nil, err
	}

	// Parse netutil listener config
	if conf.ListenerConfig, err = netutil.ListenerConfigFromParsed(pConf.Namespace("tcp")); err != nil {
		return nil, fmt.Errorf("parse tcp config: %w", err)
	}

	// Initialize HTTP-specific middleware
	rpJWT, err := gateway.NewRPJWTMiddleware(mgr)
	if err != nil {
		return nil, err
	}

	otlpIn, err := newOTLPInputFromParsed(pConf, mgr)
	if err != nil {
		return nil, err
	}
	return &httpOTLPInput{
		otlpInput: otlpIn,
		conf:      conf,
		rpJWT:     rpJWT,
		cors:      gateway.NewCORSConfigFromEnv(),
	}, nil
}

func init() {
	service.MustRegisterBatchInput("otlp_http", HTTPInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		return HTTPInputFromParsed(conf, mgr)
	})
}

//------------------------------------------------------------------------------

// Connect starts the HTTP server.
func (hi *httpOTLPInput) Connect(ctx context.Context) error {
	if hi.server != nil {
		return nil
	}

	// Initialize Schema Registry
	if err := hi.maybeInitSchemaRegistry(ctx); err != nil {
		return fmt.Errorf("initialize schema registry: %w", err)
	}

	h := hi.handler()
	h = hi.cors.WrapHandler(hi.rpJWT.Wrap(h))
	hi.server = &http.Server{
		Addr:         hi.conf.Address,
		Handler:      h,
		ReadTimeout:  hi.conf.ReadTimeout,
		WriteTimeout: hi.conf.WriteTimeout,
	}

	// Configure TLS if enabled
	if hi.conf.TLS.Enabled {
		cert, err := tls.LoadX509KeyPair(hi.conf.TLS.CertFile, hi.conf.TLS.KeyFile)
		if err != nil {
			return fmt.Errorf("load TLS certificate: %w", err)
		}
		hi.server.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	// Create listener
	var lc net.ListenConfig
	if err := netutil.DecorateListenerConfig(&lc, hi.conf.ListenerConfig); err != nil {
		return fmt.Errorf("configure listener: %w", err)
	}
	ln, err := lc.Listen(ctx, "tcp", hi.conf.Address)
	if err != nil {
		return fmt.Errorf("create HTTP listener: %w", err)
	}

	hi.log.Infof("Starting OTLP HTTP server on %s", hi.conf.Address)
	go func() {
		var serr error
		if hi.conf.TLS.Enabled {
			serr = hi.server.ServeTLS(ln, "", "")
		} else {
			serr = hi.server.Serve(ln)
		}
		if serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			hi.log.Errorf("HTTP server error: %v", serr)
		}
	}()

	return nil
}

// Close shuts down the HTTP server.
func (hi *httpOTLPInput) Close(ctx context.Context) error {
	hi.shutSig.TriggerSoftStop()
	defer hi.shutSig.TriggerHasStopped()

	if hi.server == nil {
		return nil
	}

	// Shutdown HTTP server gracefully
	ctx, cancel := context.WithTimeout(ctx, gracefulShutdownTimeout)
	defer cancel()
	if err := hi.server.Shutdown(ctx); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			hi.log.Warnf("HTTP server shutdown error: %v", err)
		}
		if err := hi.server.Close(); err != nil {
			hi.log.Warnf("HTTP server close error: %v", err)
		}
	}

	return nil
}

const (
	pbContentType   = "application/x-protobuf"
	jsonContentType = "application/json"
)

func (hi *httpOTLPInput) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hi.shutSig.IsSoftStopSignalled() {
			http.Error(w, "Server closing", http.StatusServiceUnavailable)
			return
		}

		// Validate authentication if configured
		if hi.conf.AuthToken != "" {
			authHeader := r.Header.Get("Authorization")
			expectedAuth := "Bearer " + hi.conf.AuthToken
			if subtle.ConstantTimeCompare([]byte(authHeader), []byte(expectedAuth)) != 1 {
				hi.log.Warnf("Unauthorized request from %s", r.RemoteAddr)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		// Validate URL and method
		const (
			tracesURLPath  = "/v1/traces"
			logsURLPath    = "/v1/logs"
			metricsURLPath = "/v1/metrics"
		)
		switch r.URL.Path {
		case tracesURLPath, logsURLPath, metricsURLPath:
			// continue
		default:
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Validate content type
		mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid content type: %v", err), http.StatusUnsupportedMediaType)
			return
		}
		if mt == "" {
			mt = jsonContentType
		}
		if mt != pbContentType && mt != jsonContentType {
			http.Error(w, fmt.Sprintf("unsupported media type: %s (supported: %s, %s)", mt, pbContentType, jsonContentType), http.StatusUnsupportedMediaType)
			return
		}

		// Read and parse body
		hi.maybeWaitForAccess(r.Context())

		r.Body = http.MaxBytesReader(w, r.Body, int64(hi.conf.MaxBodySize))
		defer r.Body.Close()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			hi.log.Warnf("Failed to read request body: %v", err)
			http.Error(w, "Failed to read request", http.StatusBadRequest)
			return
		}

		var obj interface {
			json.Unmarshaler
			json.Marshaler
			UnmarshalProto(data []byte) error
		}
		switch r.URL.Path {
		case tracesURLPath:
			obj = ptraceotlp.NewExportRequest()
		case logsURLPath:
			obj = plogotlp.NewExportRequest()
		case metricsURLPath:
			obj = pmetricotlp.NewExportRequest()
		default:
			panic("unreachable")
		}
		switch mt {
		case pbContentType:
			err = obj.UnmarshalProto(body)
		case jsonContentType:
			err = obj.UnmarshalJSON(body)
		default:
			panic("unreachable")
		}
		if err != nil {
			hi.log.Warnf("Failed to unmarshal request: %v", err)
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		// Convert OTLP to Redpanda protobuf using streaming API
		var batch service.MessageBatch
		var marshalErr error

		switch req := obj.(type) {
		case ptraceotlp.ExportRequest:
			if req.Traces().SpanCount() == 0 {
				w.Header().Set("Content-Type", mt)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(marshalContentType(ptraceotlp.NewExportResponse(), mt))
				return
			}

			batch = make(service.MessageBatch, 0, otlpconv.SpansCount(req))
			otlpconv.TracesToRedpandaFunc(req, func(span *pb.Span) bool {
				msg, err := hi.newMessageWithSignalType(span, SignalTypeTrace)
				if err != nil {
					marshalErr = err
					return false
				}

				batch = append(batch, msg)
				return true
			})

			if marshalErr != nil {
				hi.log.Warnf("Failed to marshal span: %v", marshalErr)
				http.Error(w, "Internal error", http.StatusInternalServerError)
				return
			}

		case plogotlp.ExportRequest:
			if req.Logs().LogRecordCount() == 0 {
				w.Header().Set("Content-Type", mt)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(marshalContentType(plogotlp.NewExportResponse(), mt))
				return
			}

			batch = make(service.MessageBatch, 0, otlpconv.LogsCount(req))
			otlpconv.LogsToRedpandaFunc(req, func(logRecord *pb.LogRecord) bool {
				msg, err := hi.newMessageWithSignalType(logRecord, SignalTypeLog)
				if err != nil {
					marshalErr = err
					return false
				}

				batch = append(batch, msg)
				return true
			})

			if marshalErr != nil {
				hi.log.Warnf("Failed to marshal log record: %v", marshalErr)
				http.Error(w, "Internal error", http.StatusInternalServerError)
				return
			}

		case pmetricotlp.ExportRequest:
			if req.Metrics().DataPointCount() == 0 {
				w.Header().Set("Content-Type", mt)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(marshalContentType(pmetricotlp.NewExportResponse(), mt))
				return
			}

			batch = make(service.MessageBatch, 0, otlpconv.MetricsCount(req))
			otlpconv.MetricsToRedpandaFunc(req, func(metric *pb.Metric) bool {
				msg, err := hi.newMessageWithSignalType(metric, SignalTypeMetric)
				if err != nil {
					marshalErr = err
					return false
				}

				batch = append(batch, msg)
				return true
			})

			if marshalErr != nil {
				hi.log.Warnf("Failed to marshal metric: %v", marshalErr)
				http.Error(w, "Internal error", http.StatusInternalServerError)
				return
			}

		default:
			panic("unreachable")
		}

		// Send batch and wait for ack
		resCh, err := hi.sendMessageBatch(r.Context(), batch)
		if err != nil {
			if errors.Is(err, service.ErrNotConnected) {
				http.Error(w, "Server closing", http.StatusServiceUnavailable)
			} else {
				http.Error(w, "Request timeout", http.StatusRequestTimeout)
			}
			return
		}

		select {
		case err := <-resCh:
			if err != nil {
				hi.log.Warnf("Pipeline error: %v", err)
				http.Error(w, "Internal error", http.StatusInternalServerError)
				return
			}
		case <-r.Context().Done():
			http.Error(w, "Request timeout", http.StatusRequestTimeout)
			return
		}

		w.Header().Set("Content-Type", mt)
		w.WriteHeader(http.StatusOK)

		var respBytes []byte
		switch r.URL.Path {
		case tracesURLPath:
			respBytes = marshalContentType(ptraceotlp.NewExportResponse(), mt)
		case logsURLPath:
			respBytes = marshalContentType(plogotlp.NewExportResponse(), mt)
		case metricsURLPath:
			respBytes = marshalContentType(pmetricotlp.NewExportResponse(), mt)
		default:
			panic("unreachable")
		}
		_, _ = w.Write(respBytes)
	})
}

func marshalContentType(resp interface {
	MarshalProto() ([]byte, error)
	MarshalJSON() ([]byte, error)
}, mt string,
) []byte {
	var b []byte
	switch mt {
	case pbContentType:
		b, _ = resp.MarshalProto()
	case jsonContentType:
		b, _ = resp.MarshalJSON()
	default:
		panic("unreachable")
	}
	return b
}
