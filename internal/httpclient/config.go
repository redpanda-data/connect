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

package httpclient

import (
	"crypto/tls"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/utils/netutil"
)

const (
	cFieldBaseURL            = "base_url"
	cFieldTimeout            = "timeout"
	cFieldTLS                = "tls"
	cFieldProxyURL           = "proxy_url"
	cFieldDisableHTTP2       = "disable_http2"
	cFieldTPSLimit           = "tps_limit"
	cFieldTPSBurst           = "tps_burst"
	cFieldBackoff            = "backoff"
	cFieldBackoffInitial     = "initial_interval"
	cFieldBackoffMax         = "max_interval"
	cFieldBackoffMaxRetries  = "max_retries"
	cFieldAccessLogLevel     = "access_log_level"
	cFieldAccessLogBodyLimit = "access_log_body_limit"

	// http transport section
	cFieldHTTP                       = "http"
	cFieldHTTPMaxIdleConns           = "max_idle_conns"
	cFieldHTTPMaxIdleConnsPerHost    = "max_idle_conns_per_host"
	cFieldHTTPMaxConnsPerHost        = "max_conns_per_host"
	cFieldHTTPIdleConnTimeout        = "idle_conn_timeout"
	cFieldHTTPTLSHandshakeTimeout    = "tls_handshake_timeout"
	cFieldHTTPExpectContinueTimeout  = "expect_continue_timeout"
	cFieldHTTPResponseHeaderTimeout  = "response_header_timeout"
	cFieldHTTPDisableKeepAlives      = "disable_keep_alives"
	cFieldHTTPDisableCompression     = "disable_compression"
	cFieldHTTPMaxResponseHeaderBytes = "max_response_header_bytes"
	cFieldHTTPMaxResponseBodyBytes   = "max_response_body_bytes"
	cFieldHTTPWriteBufferSize        = "write_buffer_size"
	cFieldHTTPReadBufferSize         = "read_buffer_size"

	// http.h2 section
	cFieldH2                            = "h2"
	cFieldH2StrictMaxConcurrentRequests = "strict_max_concurrent_requests"
	cFieldH2MaxDecoderHeaderTableSize   = "max_decoder_header_table_size"
	cFieldH2MaxEncoderHeaderTableSize   = "max_encoder_header_table_size"
	cFieldH2MaxReadFrameSize            = "max_read_frame_size"
	cFieldH2MaxRecvBufferPerConn        = "max_receive_buffer_per_connection"
	cFieldH2MaxRecvBufferPerStream      = "max_receive_buffer_per_stream"
	cFieldH2SendPingTimeout             = "send_ping_timeout"
	cFieldH2PingTimeout                 = "ping_timeout"
	cFieldH2WriteByteTimeout            = "write_byte_timeout"
)

// H2TransportConfig holds HTTP/2-specific settings that map to net/http.HTTP2Config.
type H2TransportConfig struct {
	StrictMaxConcurrentRequests   bool
	MaxDecoderHeaderTableSize     int
	MaxEncoderHeaderTableSize     int
	MaxReadFrameSize              int
	MaxReceiveBufferPerConnection int
	MaxReceiveBufferPerStream     int
	SendPingTimeout               time.Duration
	PingTimeout                   time.Duration
	WriteByteTimeout              time.Duration
}

// DefaultH2TransportConfig returns HTTP/2 transport defaults matching Go's
// internal defaults for http2.Transport.
func DefaultH2TransportConfig() H2TransportConfig {
	return H2TransportConfig{
		MaxDecoderHeaderTableSize:     4096,
		MaxEncoderHeaderTableSize:     4096,
		MaxReadFrameSize:              16384,
		MaxReceiveBufferPerConnection: 1 << 20,
		MaxReceiveBufferPerStream:     1 << 20,
		PingTimeout:                   15 * time.Second,
	}
}

// TransportConfig holds HTTP transport pool and timing settings that map
// directly to net/http.Transport fields.
type TransportConfig struct {
	MaxIdleConns           int
	MaxIdleConnsPerHost    int
	MaxConnsPerHost        int
	IdleConnTimeout        time.Duration
	TLSHandshakeTimeout    time.Duration
	ExpectContinueTimeout  time.Duration
	ResponseHeaderTimeout  time.Duration
	DisableKeepAlives      bool
	DisableCompression     bool
	MaxResponseHeaderBytes int64
	MaxResponseBodyBytes   int64
	WriteBufferSize        int
	ReadBufferSize         int
	H2                     H2TransportConfig
}

// DefaultTransportConfig returns transport defaults matching Go's
// http.DefaultTransport with MaxIdleConnsPerHost tuned to GOMAXPROCS+1.
func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		MaxIdleConns:           100,
		MaxIdleConnsPerHost:    runtime.GOMAXPROCS(0) + 1,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    10 * time.Second,
		ExpectContinueTimeout:  1 * time.Second,
		MaxResponseHeaderBytes: 1 << 20,
		MaxResponseBodyBytes:   10 << 20,
		WriteBufferSize:        4096,
		ReadBufferSize:         4096,
		H2:                     DefaultH2TransportConfig(),
	}
}

// Config holds parsed HTTP client configuration.
type Config struct {
	BaseURL      string
	Timeout      time.Duration
	TLSConf      *tls.Config
	TLSEnabled   bool
	ProxyURL     string
	DisableHTTP2 bool

	// AuthSigner is the single programmatic hook for authentication.
	// Products set this to apply their auth strategy (basic auth, bearer
	// token, OAuth2, etc.). Use the convenience constructors
	// BasicAuthSigner and BearerTokenSigner for common patterns.
	// If nil, no authentication is applied.
	AuthSigner func(fs.FS, *http.Request) error

	TPSLimit float64
	TPSBurst int

	BackoffInitialInterval time.Duration
	BackoffMaxInterval     time.Duration
	BackoffMaxRetries      int

	Dialer    netutil.DialerConfig
	Transport TransportConfig

	AccessLogLevel     string
	AccessLogBodyLimit int

	// Retry enables extended retry behavior beyond the default adaptive 429
	// backoff. When set, it governs which status codes are retried, dropped,
	// and treated as successful.
	Retry *RetryConfig

	// MetricPrefix is the prefix for benthos metrics emitted by the client.
	// If empty, no metrics are recorded.
	MetricPrefix string
}

// Fields returns the YAML configuration field specs for the HTTP client.
// Auth is not included — products configure auth programmatically via
// Config.AuthSigner (see BasicAuthSigner, BearerTokenSigner).
//
// If baseURL is non-empty it is used as the default value for the base_url
// field; otherwise the field is required (no default).
func Fields(baseURL string) []*service.ConfigField {
	baseURLField := service.NewStringField(cFieldBaseURL).
		Description("Base URL of the target service (e.g., https://api.example.com). TLS is enabled automatically for https URLs.")
	if baseURL != "" {
		baseURLField = baseURLField.Default(baseURL)
	}
	fields := []*service.ConfigField{
		baseURLField,

		service.NewDurationField(cFieldTimeout).
			Description("HTTP request timeout.").
			Default("5s"),

		service.NewTLSToggledField(cFieldTLS),

		service.NewStringField(cFieldProxyURL).
			Description("HTTP proxy URL. Empty string disables proxying.").
			Default("").
			Advanced(),

		service.NewBoolField(cFieldDisableHTTP2).
			Description("Disable HTTP/2 and force HTTP/1.1.").
			Default(false).
			Advanced(),
	}

	fields = append(fields,
		service.NewFloatField(cFieldTPSLimit).
			Description("Rate limit in requests per second. 0 disables rate limiting.").
			Default(0.0).
			Advanced(),

		service.NewIntField(cFieldTPSBurst).
			Description("Maximum burst size for rate limiting.").
			Default(1).
			Advanced(),

		service.NewObjectField(cFieldBackoff,
			service.NewDurationField(cFieldBackoffInitial).
				Description("Initial interval between retries on 429 responses.").
				Default("1s"),
			service.NewDurationField(cFieldBackoffMax).
				Description("Maximum interval between retries on 429 responses.").
				Default("30s"),
			service.NewIntField(cFieldBackoffMaxRetries).
				Description("Maximum number of retries on 429 responses.").
				Default(3),
		).Description("Adaptive backoff configuration for 429 (Too Many Requests) responses. Always active.").
			Advanced(),
		netutil.DialerConfigSpec(),
		httpTransportFieldSpec(),

		service.NewStringEnumField(cFieldAccessLogLevel, "",
			logLevelTrace.String(), logLevelDebug.String(), logLevelInfo.String(), logLevelWarn.String(), logLevelError.String()).
			Description("Log level for HTTP request/response logging. Empty disables logging.").
			Default("").
			Advanced(),

		service.NewIntField(cFieldAccessLogBodyLimit).
			Description("Maximum bytes of request/response body to include in logs. 0 to skip body logging.").
			Default(0).
			Advanced(),
	)

	return fields
}

func httpTransportFieldSpec() *service.ConfigField {
	defaults := DefaultTransportConfig()

	h2 := defaults.H2

	h2Fields := service.NewObjectField(cFieldH2,
		service.NewBoolField(cFieldH2StrictMaxConcurrentRequests).
			Description("When true, new requests block when a connection's concurrency limit is reached instead of opening a new connection.").
			Default(false),
		service.NewIntField(cFieldH2MaxDecoderHeaderTableSize).
			Description("Upper limit in bytes for the HPACK header table used to decode headers from the peer. Must be less than 4 MiB.").
			Default(h2.MaxDecoderHeaderTableSize),
		service.NewIntField(cFieldH2MaxEncoderHeaderTableSize).
			Description("Upper limit in bytes for the HPACK header table used to encode headers sent to the peer. Must be less than 4 MiB.").
			Default(h2.MaxEncoderHeaderTableSize),
		service.NewIntField(cFieldH2MaxReadFrameSize).
			Description("Largest HTTP/2 frame this endpoint will read. Valid range: 16 KiB to 16 MiB.").
			Default(h2.MaxReadFrameSize),
		service.NewIntField(cFieldH2MaxRecvBufferPerConn).
			Description("Maximum flow-control window size in bytes for data received on a connection. Must be at least 64 KiB and less than 4 MiB.").
			Default(h2.MaxReceiveBufferPerConnection),
		service.NewIntField(cFieldH2MaxRecvBufferPerStream).
			Description("Maximum flow-control window size in bytes for data received on a single stream. Must be less than 4 MiB.").
			Default(h2.MaxReceiveBufferPerStream),
		service.NewDurationField(cFieldH2SendPingTimeout).
			Description("Idle timeout after which a PING frame is sent to verify connection health. 0 disables health checks.").
			Default("0s"),
		service.NewDurationField(cFieldH2PingTimeout).
			Description("Timeout waiting for a PING response before closing the connection.").
			Default(h2.PingTimeout.String()),
		service.NewDurationField(cFieldH2WriteByteTimeout).
			Description("Timeout for writing data to a connection. The timer resets whenever bytes are written. 0 disables the timeout.").
			Default("0s"),
	).Description("HTTP/2-specific transport settings. Only applied when HTTP/2 is enabled.").
		Advanced()

	return service.NewObjectField(cFieldHTTP,
		service.NewIntField(cFieldHTTPMaxIdleConns).
			Description("Maximum total number of idle (keep-alive) connections across all hosts. 0 means unlimited.").
			Default(defaults.MaxIdleConns),
		service.NewIntField(cFieldHTTPMaxIdleConnsPerHost).
			Description("Maximum idle connections to keep per host. 0 (the default) uses GOMAXPROCS+1.").
			Default(0),
		service.NewIntField(cFieldHTTPMaxConnsPerHost).
			Description("Maximum total connections (active + idle) per host. 0 means unlimited.").
			Default(64),
		service.NewDurationField(cFieldHTTPIdleConnTimeout).
			Description("How long an idle connection remains in the pool before being closed. 0 disables the timeout.").
			Default(defaults.IdleConnTimeout.String()),
		service.NewDurationField(cFieldHTTPTLSHandshakeTimeout).
			Description("Maximum time to wait for a TLS handshake to complete. 0 disables the timeout.").
			Default(defaults.TLSHandshakeTimeout.String()),
		service.NewDurationField(cFieldHTTPExpectContinueTimeout).
			Description("Maximum time to wait for a server's 100-continue response before sending the body. 0 means the body is sent immediately.").
			Default(defaults.ExpectContinueTimeout.String()),
		service.NewDurationField(cFieldHTTPResponseHeaderTimeout).
			Description("Maximum time to wait for response headers after writing the full request. 0 disables the timeout.").
			Default("0s"),
		service.NewBoolField(cFieldHTTPDisableKeepAlives).
			Description("Disable HTTP keep-alive connections; each request uses a new connection.").
			Default(false),
		service.NewBoolField(cFieldHTTPDisableCompression).
			Description("Disable automatic decompression of gzip responses.").
			Default(false),
		service.NewIntField(cFieldHTTPMaxResponseHeaderBytes).
			Description("Maximum bytes of response headers to allow.").
			Default(int(defaults.MaxResponseHeaderBytes)),
		service.NewIntField(cFieldHTTPMaxResponseBodyBytes).
			Description("Maximum bytes of response body the client will read. The response body is wrapped with a limit reader; reads beyond this cap return EOF. 0 disables the limit.").
			Default(int(defaults.MaxResponseBodyBytes)),
		service.NewIntField(cFieldHTTPWriteBufferSize).
			Description("Size in bytes of the per-connection write buffer.").
			Default(defaults.WriteBufferSize),
		service.NewIntField(cFieldHTTPReadBufferSize).
			Description("Size in bytes of the per-connection read buffer.").
			Default(defaults.ReadBufferSize),
		h2Fields,
	).Description("HTTP transport settings controlling connection pooling, timeouts, and HTTP/2.").
		Advanced()
}

// NewConfigFromParsed parses a Config from a benthos parsed config.
func NewConfigFromParsed(pConf *service.ParsedConfig) (Config, error) {
	var cfg Config
	var err error

	if cfg.BaseURL, err = pConf.FieldString(cFieldBaseURL); err != nil {
		return cfg, err
	}
	if _, err := url.ParseRequestURI(cfg.BaseURL); err != nil {
		return cfg, fmt.Errorf("base_url is not a valid URL: %w", err)
	}

	if cfg.Timeout, err = pConf.FieldDuration(cFieldTimeout); err != nil {
		return cfg, err
	}

	if cfg.TLSConf, cfg.TLSEnabled, err = pConf.FieldTLSToggled(cFieldTLS); err != nil {
		return cfg, err
	}

	// Auto-enable TLS for https URLs when not explicitly configured.
	if !cfg.TLSEnabled && strings.HasPrefix(cfg.BaseURL, "https://") {
		cfg.TLSEnabled = true
		if cfg.TLSConf == nil {
			cfg.TLSConf = &tls.Config{MinVersion: tls.VersionTLS12}
		}
	}

	if cfg.ProxyURL, err = pConf.FieldString(cFieldProxyURL); err != nil {
		return cfg, err
	}

	if cfg.DisableHTTP2, err = pConf.FieldBool(cFieldDisableHTTP2); err != nil {
		return cfg, err
	}

	// Auth is not parsed from YAML — products set Config.AuthSigner
	// programmatically after calling NewConfigFromParsed.

	if cfg.TPSLimit, err = pConf.FieldFloat(cFieldTPSLimit); err != nil {
		return cfg, err
	}

	if cfg.TPSBurst, err = pConf.FieldInt(cFieldTPSBurst); err != nil {
		return cfg, err
	}

	backoffConf := pConf.Namespace(cFieldBackoff)
	if cfg.BackoffInitialInterval, err = backoffConf.FieldDuration(cFieldBackoffInitial); err != nil {
		return cfg, err
	}
	if cfg.BackoffMaxInterval, err = backoffConf.FieldDuration(cFieldBackoffMax); err != nil {
		return cfg, err
	}
	if cfg.BackoffMaxRetries, err = backoffConf.FieldInt(cFieldBackoffMaxRetries); err != nil {
		return cfg, err
	}

	if pConf.Contains("tcp") {
		if cfg.Dialer, err = netutil.DialerConfigFromParsed(pConf.Namespace("tcp")); err != nil {
			return cfg, err
		}
	}

	if pConf.Contains(cFieldHTTP) {
		if cfg.Transport, err = parseTransportConfig(pConf.Namespace(cFieldHTTP)); err != nil {
			return cfg, err
		}
	} else {
		cfg.Transport = DefaultTransportConfig()
	}

	if cfg.AccessLogLevel, err = pConf.FieldString(cFieldAccessLogLevel); err != nil {
		return cfg, err
	}
	if cfg.AccessLogBodyLimit, err = pConf.FieldInt(cFieldAccessLogBodyLimit); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func parseTransportConfig(pConf *service.ParsedConfig) (TransportConfig, error) {
	var tc TransportConfig
	var err error

	if tc.MaxIdleConns, err = pConf.FieldInt(cFieldHTTPMaxIdleConns); err != nil {
		return tc, err
	}
	if tc.MaxIdleConnsPerHost, err = pConf.FieldInt(cFieldHTTPMaxIdleConnsPerHost); err != nil {
		return tc, err
	}
	if tc.MaxIdleConnsPerHost == 0 {
		tc.MaxIdleConnsPerHost = runtime.GOMAXPROCS(0) + 1
	}
	if tc.MaxConnsPerHost, err = pConf.FieldInt(cFieldHTTPMaxConnsPerHost); err != nil {
		return tc, err
	}
	if tc.IdleConnTimeout, err = pConf.FieldDuration(cFieldHTTPIdleConnTimeout); err != nil {
		return tc, err
	}
	if tc.TLSHandshakeTimeout, err = pConf.FieldDuration(cFieldHTTPTLSHandshakeTimeout); err != nil {
		return tc, err
	}
	if tc.ExpectContinueTimeout, err = pConf.FieldDuration(cFieldHTTPExpectContinueTimeout); err != nil {
		return tc, err
	}
	if tc.ResponseHeaderTimeout, err = pConf.FieldDuration(cFieldHTTPResponseHeaderTimeout); err != nil {
		return tc, err
	}
	if tc.DisableKeepAlives, err = pConf.FieldBool(cFieldHTTPDisableKeepAlives); err != nil {
		return tc, err
	}
	if tc.DisableCompression, err = pConf.FieldBool(cFieldHTTPDisableCompression); err != nil {
		return tc, err
	}

	maxRespHdr, err := pConf.FieldInt(cFieldHTTPMaxResponseHeaderBytes)
	if err != nil {
		return tc, err
	}
	tc.MaxResponseHeaderBytes = int64(maxRespHdr)

	maxRespBody, err := pConf.FieldInt(cFieldHTTPMaxResponseBodyBytes)
	if err != nil {
		return tc, err
	}
	tc.MaxResponseBodyBytes = int64(maxRespBody)

	if tc.WriteBufferSize, err = pConf.FieldInt(cFieldHTTPWriteBufferSize); err != nil {
		return tc, err
	}
	if tc.ReadBufferSize, err = pConf.FieldInt(cFieldHTTPReadBufferSize); err != nil {
		return tc, err
	}

	if pConf.Contains(cFieldH2) {
		if tc.H2, err = parseH2Config(pConf.Namespace(cFieldH2)); err != nil {
			return tc, err
		}
	}

	return tc, nil
}

func parseH2Config(pConf *service.ParsedConfig) (H2TransportConfig, error) {
	var h2 H2TransportConfig
	var err error

	if h2.StrictMaxConcurrentRequests, err = pConf.FieldBool(cFieldH2StrictMaxConcurrentRequests); err != nil {
		return h2, err
	}
	if h2.MaxDecoderHeaderTableSize, err = pConf.FieldInt(cFieldH2MaxDecoderHeaderTableSize); err != nil {
		return h2, err
	}
	if h2.MaxEncoderHeaderTableSize, err = pConf.FieldInt(cFieldH2MaxEncoderHeaderTableSize); err != nil {
		return h2, err
	}
	if h2.MaxReadFrameSize, err = pConf.FieldInt(cFieldH2MaxReadFrameSize); err != nil {
		return h2, err
	}
	if h2.MaxReceiveBufferPerConnection, err = pConf.FieldInt(cFieldH2MaxRecvBufferPerConn); err != nil {
		return h2, err
	}
	if h2.MaxReceiveBufferPerStream, err = pConf.FieldInt(cFieldH2MaxRecvBufferPerStream); err != nil {
		return h2, err
	}
	if h2.SendPingTimeout, err = pConf.FieldDuration(cFieldH2SendPingTimeout); err != nil {
		return h2, err
	}
	if h2.PingTimeout, err = pConf.FieldDuration(cFieldH2PingTimeout); err != nil {
		return h2, err
	}
	if h2.WriteByteTimeout, err = pConf.FieldDuration(cFieldH2WriteByteTimeout); err != nil {
		return h2, err
	}

	if err := validateH2Config(h2); err != nil {
		return h2, err
	}

	return h2, nil
}

const (
	h2MaxHeaderTableSize = 4 << 20  // 4 MiB
	h2MinReadFrameSize   = 16 << 10 // 16 KiB
	h2MaxReadFrameSize   = 16 << 20 // 16 MiB
	h2MinRecvBuffer      = 64 << 10 // 64 KiB
	h2MaxRecvBuffer      = 4 << 20  // 4 MiB
)

func validateH2Config(h2 H2TransportConfig) error {
	if h2.MaxDecoderHeaderTableSize >= h2MaxHeaderTableSize {
		return fmt.Errorf("h2.max_decoder_header_table_size must be less than 4 MiB, got %d", h2.MaxDecoderHeaderTableSize)
	}
	if h2.MaxEncoderHeaderTableSize >= h2MaxHeaderTableSize {
		return fmt.Errorf("h2.max_encoder_header_table_size must be less than 4 MiB, got %d", h2.MaxEncoderHeaderTableSize)
	}
	if h2.MaxReadFrameSize < h2MinReadFrameSize || h2.MaxReadFrameSize > h2MaxReadFrameSize {
		return fmt.Errorf("h2.max_read_frame_size must be between 16 KiB and 16 MiB, got %d", h2.MaxReadFrameSize)
	}
	if h2.MaxReceiveBufferPerConnection < h2MinRecvBuffer || h2.MaxReceiveBufferPerConnection >= h2MaxRecvBuffer {
		return fmt.Errorf("h2.max_receive_buffer_per_connection must be between 64 KiB and less than 4 MiB, got %d", h2.MaxReceiveBufferPerConnection)
	}
	if h2.MaxReceiveBufferPerStream >= h2MaxRecvBuffer {
		return fmt.Errorf("h2.max_receive_buffer_per_stream must be less than 4 MiB, got %d", h2.MaxReceiveBufferPerStream)
	}
	return nil
}

// RetryConfig controls retry behavior for the HTTP client. This is a Go API
// config, not exposed via YAML fields.
type RetryConfig struct {
	MaxRetries      int
	RetryStatuses   []int // status codes that trigger backoff retry
	DropStatuses    []int // status codes that immediately fail (no retry)
	SuccessStatuses []int // status codes treated as success
	InitialInterval time.Duration
	MaxInterval     time.Duration
}

// DefaultRetryConfig returns sensible retry defaults.
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:      3,
		RetryStatuses:   []int{429, 502, 503, 504},
		DropStatuses:    []int{401, 403},
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     30 * time.Second,
	}
}

func (rc *RetryConfig) normalize() {
	slices.Sort(rc.RetryStatuses)
	slices.Sort(rc.DropStatuses)
	slices.Sort(rc.SuccessStatuses)
}

// BasicAuthSigner returns an AuthSigner that sets HTTP Basic Authentication
// on every request.
func BasicAuthSigner(username, password string) func(fs.FS, *http.Request) error {
	return func(_ fs.FS, req *http.Request) error {
		req.SetBasicAuth(username, password)
		return nil
	}
}

// BearerTokenSigner returns an AuthSigner that sets a static Bearer token
// in the Authorization header on every request.
func BearerTokenSigner(token string) func(fs.FS, *http.Request) error {
	return func(_ fs.FS, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	}
}
