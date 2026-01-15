// Copyright 2026 Redpanda Data, Inc.
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

package otlp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/utils/netutil"
	"github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp/otlpconv"
)

const (
	hoFieldEndpoint        = "endpoint"
	hoFieldContentType     = "content_type"
	hoFieldTimeout         = "timeout"
	hoFieldProxyURL        = "proxy_url"
	hoFieldFollowRedirects = "follow_redirects"
	hoFieldDisableHTTP2    = "disable_http2"
	hoFieldTLS             = "tls"

	defaultContentType = "protobuf"
)

type httpOutputConfig struct {
	Endpoint        string
	ContentType     string
	AuthToken       string
	Timeout         time.Duration
	ProxyURL        string
	FollowRedirects bool
	DisableHTTP2    bool
	AuthSigner      func(*http.Request) error
	TLS             tlsClientConfig
	DialerConfig    netutil.DialerConfig
}

// HTTPOutputSpec returns the configuration spec for the OTLP HTTP output.
func HTTPOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Version("4.78.0").
		Summary("Send OpenTelemetry traces, logs, and metrics via OTLP/HTTP protocol.").
		Description(`
Sends OpenTelemetry telemetry data to a remote collector via OTLP/HTTP protocol.

Accepts batches of Redpanda OTEL v1 protobuf messages (spans, log records, or metrics) and converts them to OTLP format for transmission to OpenTelemetry collectors.

## Input Format

Expects messages in Redpanda OTEL v1 protobuf format with metadata:
- `+"`signal_type`"+`: "trace", "log", or "metric"

Each batch must contain messages of the same signal type. The entire batch is converted to a single OTLP export request and sent via HTTP POST.

## Endpoints

The output automatically appends the signal type path to the base endpoint:
- Traces: `+"`{endpoint}/v1/traces`"+`
- Logs: `+"`{endpoint}/v1/logs`"+`
- Metrics: `+"`{endpoint}/v1/metrics`"+`

## Content Types

Supports two content types:
- `+"`protobuf`"+` (default): `+"`application/x-protobuf`"+`
- `+"`json`"+`: `+"`application/json`"+`

## Authentication

Supports multiple authentication methods:
- Basic authentication
- OAuth v1
- JWT
`).
		Fields(
			service.NewStringField(hoFieldEndpoint).
				Description("The HTTP endpoint of the remote OTLP collector (without the signal path)."),
			service.NewStringEnumField(hoFieldContentType, "protobuf", "json").
				Description("Content type for HTTP requests. Options: 'protobuf' or 'json'.").
				Default(defaultContentType).
				Advanced(),
			service.NewDurationField(hoFieldTimeout).
				Description("Timeout for HTTP requests.").
				Default("30s").
				Advanced(),
			service.NewStringField(hoFieldProxyURL).
				Description("An optional HTTP proxy URL.").
				Advanced().
				Default(""),
			service.NewBoolField(hoFieldFollowRedirects).
				Description("Transparently follow redirects, i.e. responses with 300-399 status codes. "+
					"If disabled, the response message will contain the body, status, and headers from the redirect response and the processor will not make a request to the URL set in the Location header of the response.").
				Advanced().
				Default(false),
			service.NewBoolField(hoFieldDisableHTTP2).
				Description("Whether or not to disable HTTP/2.").
				Advanced().
				Default(false),
			service.NewObjectField(hoFieldTLS,
				tlsClientConfigFields()...,
			).Description("TLS configuration for HTTP client.").
				Advanced().
				Optional(),
			netutil.DialerConfigSpec(),
		).
		Fields(service.NewHTTPRequestAuthSignerFields()...).
		Fields(service.NewOutputMaxInFlightField())
}

//------------------------------------------------------------------------------

type httpOTLPOutput struct {
	otlpOutput

	conf        httpOutputConfig
	client      *http.Client
	tracesURL   string
	logsURL     string
	metricsURL  string
	contentType string
}

// HTTPOutputFromParsed creates an OTLP HTTP output from a parsed config.
func HTTPOutputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, error) {
	var (
		conf httpOutputConfig
		err  error
	)

	// Parse HTTP-specific config
	if conf.Endpoint, err = pConf.FieldString(hoFieldEndpoint); err != nil {
		return nil, err
	}
	conf.Endpoint = strings.TrimSuffix(conf.Endpoint, "/")

	if conf.ContentType, err = pConf.FieldString(hoFieldContentType); err != nil {
		return nil, err
	}
	if conf.Timeout, err = pConf.FieldDuration(hoFieldTimeout); err != nil {
		return nil, err
	}
	if conf.ProxyURL, err = pConf.FieldString(hoFieldProxyURL); err != nil {
		return nil, err
	}
	if conf.FollowRedirects, err = pConf.FieldBool(hoFieldFollowRedirects); err != nil {
		return nil, err
	}
	if conf.DisableHTTP2, err = pConf.FieldBool(hoFieldDisableHTTP2); err != nil {
		return nil, err
	}

	// Parse auth configuration
	authSigner, err := pConf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, fmt.Errorf("parse auth config: %w", err)
	}
	conf.AuthSigner = func(req *http.Request) error {
		return authSigner(nil, req)
	}

	// Parse TLS config
	if pConf.Contains(hoFieldTLS) {
		if conf.TLS, err = parseTLSClientConfig(pConf.Namespace(hoFieldTLS)); err != nil {
			return nil, err
		}
	}

	// Parse netutil dialer config
	if pConf.Contains("tcp") {
		if conf.DialerConfig, err = netutil.DialerConfigFromParsed(pConf.Namespace("tcp")); err != nil {
			return nil, fmt.Errorf("parse tcp config: %w", err)
		}
	}

	// Determine paths for each signal type
	tracesURL, err := url.JoinPath(conf.Endpoint, "/v1/traces")
	if err != nil {
		return nil, fmt.Errorf("construct traces URL: %w", err)
	}
	logsURL, err := url.JoinPath(conf.Endpoint, "/v1/logs")
	if err != nil {
		return nil, fmt.Errorf("construct logs URL: %w", err)
	}
	metricsURL, err := url.JoinPath(conf.Endpoint, "/v1/metrics")
	if err != nil {
		return nil, fmt.Errorf("construct metrics URL: %w", err)
	}

	// Determine content type header
	var contentType string
	switch conf.ContentType {
	case "protobuf":
		contentType = pbContentType
	case "json":
		contentType = jsonContentType
	default:
		return nil, fmt.Errorf("invalid content_type: %s", conf.ContentType)
	}

	return &httpOTLPOutput{
		otlpOutput: newOTLPOutput(mgr),
		conf:       conf,

		tracesURL:   tracesURL,
		logsURL:     logsURL,
		metricsURL:  metricsURL,
		contentType: contentType,
	}, nil
}

func init() {
	service.MustRegisterBatchOutput("otlp_http", HTTPOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			o service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if o, err = HTTPOutputFromParsed(conf, mgr); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}

			return
		})
}

//------------------------------------------------------------------------------

// Connect initializes the HTTP client.
func (o *httpOTLPOutput) Connect(ctx context.Context) error {
	if o.client != nil {
		return nil
	}

	// Configure custom dialer with TCP options
	var nd net.Dialer
	if err := netutil.DecorateDialer(&nd, o.conf.DialerConfig); err != nil {
		return fmt.Errorf("configure custom dialer: %w", err)
	}

	tr := &http.Transport{
		ForceAttemptHTTP2: !o.conf.DisableHTTP2,
		DialContext:       nd.DialContext,
	}
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

		tr.TLSClientConfig = tlsConf
	}
	if o.conf.ProxyURL != "" {
		proxyURL, err := url.Parse(o.conf.ProxyURL)
		if err != nil {
			return fmt.Errorf("failed to parse proxy_url string: %w", err)
		}
		tr.Proxy = http.ProxyURL(proxyURL)
	}
	o.client = &http.Client{
		Transport: tr,
		Timeout:   o.conf.Timeout,
	}
	if !o.conf.FollowRedirects {
		o.client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}

	o.log.Infof("Connected to OTLP HTTP endpoint: %s", o.conf.Endpoint)
	return nil
}

// WriteBatch converts and sends a batch of messages to the remote collector.
func (o *httpOTLPOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
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

func (o *httpOTLPOutput) sendTraces(ctx context.Context, batch service.MessageBatch) error {
	spans, err := unmarshalBatch[proto.Span](batch, "span")
	if err != nil {
		return fmt.Errorf("unmarshal spans: %w", err)
	}

	body := marshalContentType(otlpconv.TracesFromRedpanda(spans), o.contentType)
	return o.sendHTTPRequest(ctx, SignalTypeTrace, body)
}

func (o *httpOTLPOutput) sendLogs(ctx context.Context, batch service.MessageBatch) error {
	logs, err := unmarshalBatch[proto.LogRecord](batch, "log record")
	if err != nil {
		return fmt.Errorf("unmarshal logs: %w", err)
	}

	body := marshalContentType(otlpconv.LogsFromRedpanda(logs), o.contentType)
	return o.sendHTTPRequest(ctx, SignalTypeLog, body)
}

func (o *httpOTLPOutput) sendMetrics(ctx context.Context, batch service.MessageBatch) error {
	metrics, err := unmarshalBatch[proto.Metric](batch, "metric")
	if err != nil {
		return fmt.Errorf("unmarshal metrics: %w", err)
	}

	body := marshalContentType(otlpconv.MetricsFromRedpanda(metrics), o.contentType)
	return o.sendHTTPRequest(ctx, SignalTypeMetric, body)
}

func (o *httpOTLPOutput) sendHTTPRequest(
	ctx context.Context,
	signalType SignalType,
	body []byte,
) error {
	var url string
	switch signalType {
	case SignalTypeTrace:
		url = o.tracesURL
	case SignalTypeLog:
		url = o.logsURL
	case SignalTypeMetric:
		url = o.metricsURL
	default:
		panic("unreachable: invalid signal type")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", o.contentType)

	// Apply authentication
	if o.conf.AuthSigner != nil {
		if err := o.conf.AuthSigner(req); err != nil {
			return fmt.Errorf("sign HTTP request: %w", err)
		}
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("send HTTP request: %w", err)
	}
	return o.handleResponse(signalType, resp)
}

func (o *httpOTLPOutput) handleResponse(signalType SignalType, resp *http.Response) error {
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Discard response body on error to allow connection reuse
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			o.log.Warnf("Failed to discard response body: %v", err)
		}
		return fmt.Errorf("unexpected HTTP status: %d %s", resp.StatusCode, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	var obj interface {
		json.Unmarshaler
		UnmarshalProto(data []byte) error
	}
	switch signalType {
	case SignalTypeTrace:
		obj = ptraceotlp.NewExportResponse()
	case SignalTypeLog:
		obj = plogotlp.NewExportResponse()
	case SignalTypeMetric:
		obj = pmetricotlp.NewExportResponse()
	default:
		panic("unreachable")
	}
	switch o.contentType {
	case pbContentType:
		err = obj.UnmarshalProto(body)
	case jsonContentType:
		err = obj.UnmarshalJSON(body)
	default:
		panic("unreachable")
	}
	if err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}

	switch r := obj.(type) {
	case ptraceotlp.ExportResponse:
		if s := r.PartialSuccess(); s.RejectedSpans() > 0 {
			return fmt.Errorf("export traces: %d spans were rejected by the collector: %s",
				s.RejectedSpans(), s.ErrorMessage())
		}
	case plogotlp.ExportResponse:
		if s := r.PartialSuccess(); s.RejectedLogRecords() > 0 {
			return fmt.Errorf("export logs: %d log records were rejected by the collector: %s",
				s.RejectedLogRecords(), s.ErrorMessage())
		}
	case pmetricotlp.ExportResponse:
		if s := r.PartialSuccess(); s.RejectedDataPoints() > 0 {
			return fmt.Errorf("export metrics: %d metrics were rejected by the collector: %s",
				s.RejectedDataPoints(), s.ErrorMessage())
		}
	default:
		panic("unreachable")
	}

	return nil
}

// Close closes the HTTP client (no-op for HTTP transport).
func (o *httpOTLPOutput) Close(_ context.Context) error {
	if o.client != nil {
		o.client.CloseIdleConnections()
	}
	return nil
}
