// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/klauspost/compress/gzip"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/utils/netutil"
	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/connect/v4/internal/gateway"
)

const (
	hsiFieldPath                    = "path"
	hsiFieldRateLimit               = "rate_limit"
	hsiFieldResponse                = "sync_response"
	hsiFieldResponseStatus          = "status"
	hsiFieldResponseHeaders         = "headers"
	hsiFieldResponseExtractMetadata = "metadata_headers"
	hsiFieldAuth                    = "auth"
	hsiFieldHMAC                    = "hmac"
	hsiFieldHMACSecret              = "secret"
	hsiFieldHMACHeader              = "header"
	hsiFieldHMACPrefix              = "prefix"
	hsiFieldHMACAlgorithm           = "algorithm"
	hsiFieldHMACMaxBodySize         = "max_body_size"
)

// defaultHMACMaxBodySize caps the pre-authentication request body buffering
// performed by HMAC signature verification, which reads the full body before
// a caller has proven ownership of the shared secret.
const defaultHMACMaxBodySize = 4 * 1024 * 1024 // 4MB

// Gateway HTTP authorization permission
const gatewayPermission authz.PermissionName = "dataplane_pipeline_gateway_invoke"

type hsiConfig struct {
	Path      string
	RateLimit string
	Response  hsiResponseConfig
	Auth      authConfig

	// Set via environment variables
	Address string
	CORS    gateway.CORSConfig
}

// authConfig holds the set of supported request authentication mechanisms
// that, when configured, replace the platform-managed (Redpanda Cloud
// JWT/RBAC) authentication for this endpoint. Exactly one mechanism may be
// set at a time. HMAC is the only mechanism supported today; additional
// mechanisms should be added here as further fields alongside HMAC.
type authConfig struct {
	HMAC hmacConfig
}

type hmacConfig struct {
	Enabled     bool
	Secret      string
	Header      string
	Prefix      string
	Algorithm   string
	MaxBodySize int
}

type hsiResponseConfig struct {
	Status          *service.InterpolatedString
	Headers         map[string]*service.InterpolatedString
	ExtractMetadata *service.MetadataFilter
}

func hsiConfigFromParsed(pConf *service.ParsedConfig) (conf hsiConfig, err error) {
	if conf.Path, err = pConf.FieldString(hsiFieldPath); err != nil {
		return
	}
	if conf.RateLimit, err = pConf.FieldString(hsiFieldRateLimit); err != nil {
		return
	}
	if conf.Response, err = hsiResponseConfigFromParsed(pConf.Namespace(hsiFieldResponse)); err != nil {
		return
	}
	if pConf.Contains(hsiFieldAuth) {
		if conf.Auth, err = hsiAuthConfigFromParsed(pConf.Namespace(hsiFieldAuth)); err != nil {
			return
		}
	}
	return
}

// hsiAuthConfigFromParsed parses the auth block. With a single supported
// mechanism (HMAC) "at most one mechanism set" holds trivially; if a second
// mechanism is added here, add a check that rejects setting more than one.
func hsiAuthConfigFromParsed(pConf *service.ParsedConfig) (conf authConfig, err error) {
	if pConf.Contains(hsiFieldHMAC) {
		if conf.HMAC, err = hsiHMACConfigFromParsed(pConf.Namespace(hsiFieldHMAC)); err != nil {
			return
		}
		conf.HMAC.Enabled = true
	}
	return
}

func hsiHMACConfigFromParsed(pConf *service.ParsedConfig) (conf hmacConfig, err error) {
	if conf.Secret, err = pConf.FieldString(hsiFieldHMACSecret); err != nil {
		return
	}
	if conf.Header, err = pConf.FieldString(hsiFieldHMACHeader); err != nil {
		return
	}
	if conf.Prefix, err = pConf.FieldString(hsiFieldHMACPrefix); err != nil {
		return
	}
	if conf.Algorithm, err = pConf.FieldString(hsiFieldHMACAlgorithm); err != nil {
		return
	}
	if conf.MaxBodySize, err = pConf.FieldInt(hsiFieldHMACMaxBodySize); err != nil {
		return
	}
	return
}

const (
	rpEnvAddress = "REDPANDA_CLOUD_GATEWAY_ADDRESS"
)

func (h *hsiConfig) applyEnvVarOverrides() error {
	if h.Address = os.Getenv(rpEnvAddress); h.Address == "" {
		return errors.New("an address must be specified via env var for this input to be functional")
	}

	h.CORS = gateway.NewCORSConfigFromEnv()

	return nil
}

func hsiResponseConfigFromParsed(pConf *service.ParsedConfig) (conf hsiResponseConfig, err error) {
	if conf.Status, err = pConf.FieldInterpolatedString(hsiFieldResponseStatus); err != nil {
		return
	}
	if conf.Headers, err = pConf.FieldInterpolatedStringMap(hsiFieldResponseHeaders); err != nil {
		return
	}
	if conf.ExtractMetadata, err = pConf.FieldMetadataFilter(hsiFieldResponseExtractMetadata); err != nil {
		return
	}
	return
}

// InputSpec defines the config spec of an RPIngressInput.
func InputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary(`Receive messages delivered over HTTP.`).
		Description(`
The field `+"`rate_limit`"+` allows you to specify an optional `+"xref:components:rate_limits/about.adoc[`rate_limit` resource]"+`, which will be applied to each HTTP request made and each websocket payload received.

When the rate limit is breached HTTP requests will have a 429 response returned with a Retry-After header.

== Responses

It's possible to return a response for each message received using xref:guides:sync_responses.adoc[synchronous responses]. When doing so you can customize headers with the `+"`sync_response` field `headers`"+`, which can also use xref:configuration:interpolation.adoc#bloblang-queries[function interpolation] in the value based on the response message contents.

== Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- http_server_user_agent
- http_server_request_path
- http_server_verb
- http_server_remote_ip
- All headers (only first values are taken)
- All query parameters
- All path parameters
- All cookies
`+"```"+`

You can access these metadata fields using xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].

== Authentication

By default, requests to this endpoint are authenticated using the platform-managed Redpanda Cloud JWT/RBAC mechanism. Setting a mechanism under the `+"`auth`"+` field replaces this default authentication for the endpoint. `+"`hmac`"+` is the currently supported mechanism, which verifies a hex-encoded HMAC signature of the raw request body against a shared secret. This is intended for webhook-style callers that sign their payloads (for example, https://developer.hashicorp.com/terraform/cloud-docs/workspaces/settings/run-tasks[Terraform Cloud Run Tasks], which send a hex-encoded HMAC-SHA512 of the request body in a request header).

The `+"`hmac`"+` mechanism only authenticates the raw request body, and provides no replay protection: since there is no timestamp or nonce validation, anyone who captures a signed payload can resend it later. Request headers, query parameters, path parameters and cookies are not covered by the signature, even though they still become message metadata, so pipelines must not make authorization decisions based on that metadata alone.

Providers that prefix their signature header value with the algorithm name, such as GitHub's and Meta's `+"`sha256=`"+`, are supported via the `+"`prefix`"+` field, which strips the configured prefix before the remainder is hex-decoded.`).
		Fields(
			service.NewStringField(hsiFieldPath).
				Description("The endpoint path to listen for data delivery requests.").
				Default("/"),
			service.NewStringField(hsiFieldRateLimit).
				Description("An optional xref:components:rate_limits/about.adoc[rate limit] to throttle requests by.").
				ShortDescription("An optional rate limit resource to throttle requests by.").
				Default(""),
			service.NewObjectField(hsiFieldResponse,
				service.NewInterpolatedStringField(hsiFieldResponseStatus).
					Description("Specify the status code to return with synchronous responses. This is a string value, which allows you to customize it based on resulting payloads and their metadata.").
					ShortDescription("The status code returned with synchronous responses.").
					Examples(`${! json("status") }`, `${! meta("status") }`).
					Default("200"),
				service.NewInterpolatedStringMapField(hsiFieldResponseHeaders).
					Description("Specify headers to return with synchronous responses.").
					Default(map[string]any{
						"Content-Type": "application/octet-stream",
					}),
				service.NewMetadataFilterField(hsiFieldResponseExtractMetadata).
					Description("Specify criteria for which metadata values are added to the response as headers."),
			),
			service.NewObjectField(hsiFieldAuth,
				service.NewObjectField(hsiFieldHMAC,
					service.NewStringField(hsiFieldHMACSecret).
						Description("The shared secret key used to compute and verify the HMAC signature.").
						Secret().
						LintRule(`root = if this == "" { [ "a non-empty secret is required" ] }`),
					service.NewStringField(hsiFieldHMACHeader).
						Description("The name of the request header containing the HMAC signature of the request body. By default, the header value must be the bare hex-encoded signature; use `prefix` if the caller adds a fixed prefix such as `sha256=` before the signature.").
						ShortDescription("The name of the request header containing the hex-encoded HMAC signature of the request body.").
						Examples("X-Tfc-Task-Signature", "X-Hub-Signature-256"),
					service.NewStringField(hsiFieldHMACPrefix).
						Description("An optional exact-match prefix that the `header` value must begin with; it is stripped before the remaining value is hex-decoded. Some providers, such as GitHub and Meta, prefix their signature header value with the algorithm name (for example `sha256=`). This prefix is matched literally and does not influence which algorithm (`algorithm`) is used to verify the signature.").
						ShortDescription("An optional prefix stripped from the signature header value before hex decoding.").
						Examples("sha256=").
						Default(""),
					service.NewStringAnnotatedEnumField(hsiFieldHMACAlgorithm, map[string]string{
						"sha256": "Verify signatures computed with HMAC-SHA256.",
						"sha512": "Verify signatures computed with HMAC-SHA512.",
					}).
						Description("The hash algorithm used to compute the HMAC signature.").
						Default("sha512"),
					service.NewIntField(hsiFieldHMACMaxBodySize).
						Description("The maximum size of the request body in bytes that will be read and buffered for signature verification. Requests with larger bodies are rejected with 413 Request Entity Too Large.").
						Default(defaultHMACMaxBodySize).
						Advanced().
						LintRule(`root = if this <= 0 { [ "max_body_size must be greater than zero" ] }`),
				).
					Description(`
Verifies incoming requests by computing an HMAC of the raw request body using `+"`secret`"+` and comparing it against the hex-encoded signature found in the request header named by `+"`header`"+`.

This is intended for webhook-style callers that sign their payloads, such as Terraform Cloud Run Tasks, which send a hex-encoded HMAC-SHA512 of the request body in a request header. Callers that prefix the signature with a fixed value, such as GitHub's and Meta's `+"`sha256=`"+`, are supported via `+"`prefix`"+`; the algorithm used for verification always comes from `+"`algorithm`"+` and is never inferred from the header.

Only the raw request body is covered by the signature, and there is no replay protection (no timestamp or nonce is validated), so a captured signed payload can be resent. Headers, query parameters, path parameters and cookies are not signed but are still exposed as message metadata, so avoid basing authorization decisions on them.`).
					Optional(),
			).
				Description(`
Configures how incoming requests to this endpoint are authenticated. At most one authentication mechanism may be set. When a mechanism is set, it replaces the platform-managed (Redpanda Cloud JWT/RBAC) authentication for this endpoint. `+"`hmac`"+` is the currently supported mechanism.`).
				Optional().
				Advanced(),
			netutil.ListenerConfigSpec().
				Description("Customize messages returned via xref:guides:sync_responses.adoc[synchronous responses].").
				ShortDescription("Customize messages returned via synchronous responses.").
				Advanced(),
		)
}

func init() {
	service.MustRegisterBatchInput(
		"gateway", InputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return InputFromParsed(conf, mgr)
		})
}

//------------------------------------------------------------------------------

type batchAndAck struct {
	batch service.MessageBatch
	aFn   service.AckFunc
}

// Input implements service.BatchInput.
type Input struct {
	conf hsiConfig
	log  *service.Logger
	mgr  *service.Resources

	lc     netutil.ListenerConfig
	mux    *mux.Router
	server *http.Server

	rpJWTValidator *gateway.RPJWTMiddleware
	authzPolicy    *gateway.FileWatchingAuthzResourcePolicy
	hmacMiddleware *gateway.HMACMiddleware

	batches chan batchAndAck

	shutSig *shutdown.Signaller
}

// InputFromParsed returns an RPIngressInput from a parsed config.
func InputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*Input, error) {
	conf, err := hsiConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	if err := conf.applyEnvVarOverrides(); err != nil {
		return nil, err
	}

	h := Input{
		shutSig: shutdown.NewSignaller(),
		conf:    conf,
		log:     mgr.Logger(),
		mgr:     mgr,
		batches: make(chan batchAndAck),
	}
	if h.conf.Auth.HMAC.Enabled {
		if h.hmacMiddleware, err = gateway.NewHMACMiddleware(mgr, gateway.HMACConfig{
			Secret:      h.conf.Auth.HMAC.Secret,
			Header:      h.conf.Auth.HMAC.Header,
			Algorithm:   h.conf.Auth.HMAC.Algorithm,
			Prefix:      h.conf.Auth.HMAC.Prefix,
			MaxBodySize: h.conf.Auth.HMAC.MaxBodySize,
		}); err != nil {
			return nil, err
		}
		if gateway.PlatformJWTConfigured() {
			mgr.Logger().Warn("The configured auth.hmac mechanism replaces the platform-managed JWT/RBAC authentication for this endpoint.")
		}
	} else {
		// The JWT validator and authorization policy are only used by
		// createHandler in the non-HMAC branch; skip constructing them
		// entirely in HMAC mode, since the authz policy setup opens a live
		// file watch or gRPC stream that would otherwise sit unused.
		if h.rpJWTValidator, err = gateway.NewRPJWTMiddleware(mgr); err != nil {
			return nil, err
		}
		if authzConf, ok := gateway.ManagerAuthzConfig(mgr); ok {
			errorCallback := func(err error) {
				mgr.Logger().With("error", err).Error("Authorization policy error")
			}
			if authzConf.PolicyEndpoint != "" {
				h.authzPolicy, err = gateway.NewEndpointWatchingAuthzResourcePolicy(
					authzConf.ResourceName,
					authzConf.PolicyEndpoint,
					[]authz.PermissionName{gatewayPermission},
					errorCallback,
				)
			} else if authzConf.PolicyFile != "" {
				h.authzPolicy, err = gateway.NewFileWatchingAuthzResourcePolicy(
					authzConf.ResourceName,
					authzConf.PolicyFile,
					[]authz.PermissionName{gatewayPermission},
					errorCallback,
				)
			}
			if err != nil {
				return nil, fmt.Errorf("initialize authorization policy: %w", err)
			}
		}
	}

	if h.conf.RateLimit != "" {
		if !h.mgr.HasRateLimit(h.conf.RateLimit) {
			return nil, fmt.Errorf("rate limit resource '%v' was not found", h.conf.RateLimit)
		}
	}

	if h.lc, err = netutil.ListenerConfigFromParsed(pConf.Namespace("tcp")); err != nil {
		return nil, fmt.Errorf("parse tcp config: %w", err)
	}

	return &h, nil
}

//------------------------------------------------------------------------------

func (ri *Input) createHandler() (h http.Handler) {
	h = http.HandlerFunc(ri.deliverHandler)
	h = gzipHandler(h)
	// Exactly one auth mechanism applies to a given endpoint: any mechanism
	// configured under `auth` replaces the JWT/RBAC authentication chain
	// entirely. Additional mechanisms should be added as further cases here.
	switch {
	case ri.hmacMiddleware != nil:
		h = ri.hmacMiddleware.Wrap(h)
	default:
		if ri.authzPolicy != nil {
			h = gateway.AuthzMiddleware(ri.authzPolicy, gatewayPermission, h)
		}
		h = ri.rpJWTValidator.Wrap(h)
	}
	h = ri.conf.CORS.WrapHandler(h)
	return
}

// RegisterCustomMux adds the server endpoint to a mux instead of running its
// own server, this is for testing purposes only.
func (ri *Input) RegisterCustomMux(mux *mux.Router) error {
	mux.PathPrefix(ri.conf.Path).Handler(ri.createHandler())
	return nil
}

// Connect attempts to run a server with the appropriate endpoints registered
// for receiving data.
func (ri *Input) Connect(_ context.Context) error {
	if ri.server != nil {
		return nil
	}

	ri.mux = mux.NewRouter()
	ri.mux.PathPrefix(ri.conf.Path).Handler(ri.createHandler())

	var lc net.ListenConfig
	if err := netutil.DecorateListenerConfig(&lc, ri.lc); err != nil {
		return fmt.Errorf("configuring listener: %w", err)
	}

	l, err := lc.Listen(context.Background(), "tcp", ri.conf.Address)
	if err != nil {
		return fmt.Errorf("binding to address %s: %w", ri.conf.Address, err)
	}
	ri.server = &http.Server{Addr: ri.conf.Address, Handler: ri.mux}

	go func() {
		defer ri.shutSig.TriggerHasStopped()
		ri.log.With("address", ri.conf.Address+ri.conf.Path).Info("Receiving HTTP messages")
		if err := ri.server.Serve(l); errors.Is(err, http.ErrServerClosed) {
			ri.log.With("error", err).Error("Server error")
		}
	}()
	return nil
}

// ReadBatch attempts to read a batch of data received via the server endpoints.
func (ri *Input) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case <-ctx.Done():
	case baa := <-ri.batches:
		return baa.batch, baa.aFn, nil
	}
	return nil, nil, ctx.Err()
}

func extractBatchFromRequest(r *http.Request) (service.MessageBatch, error) {
	var batch service.MessageBatch

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, fmt.Errorf("parsing media type: %w", err)
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			var p *multipart.Part
			if p, err = mr.NextPart(); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, fmt.Errorf("obtaining next multipart message part: %w", err)
			}
			var msgBytes []byte
			if msgBytes, err = io.ReadAll(p); err != nil {
				return nil, fmt.Errorf("reading multipart message part: %w", err)
			}
			batch = append(batch, service.NewMessage(msgBytes))
		}
	} else {
		var msgBytes []byte
		if msgBytes, err = io.ReadAll(r.Body); err != nil {
			return nil, fmt.Errorf("reading body: %w", err)
		}
		batch = append(batch, service.NewMessage(msgBytes))
	}

	for _, p := range batch {
		p.MetaSetMut("http_server_user_agent", r.UserAgent())
		p.MetaSetMut("http_server_request_path", r.URL.Path)
		p.MetaSetMut("http_server_verb", r.Method)
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			p.MetaSetMut("http_server_remote_ip", host)
		}

		if r.TLS != nil {
			var tlsVersion string
			switch r.TLS.Version {
			case tls.VersionTLS10:
				tlsVersion = "TLSv1.0"
			case tls.VersionTLS11:
				tlsVersion = "TLSv1.1"
			case tls.VersionTLS12:
				tlsVersion = "TLSv1.2"
			case tls.VersionTLS13:
				tlsVersion = "TLSv1.3"
			}
			p.MetaSetMut("http_server_tls_version", tlsVersion)
			if len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
				p.MetaSetMut("http_server_tls_subject", r.TLS.VerifiedChains[0][0].Subject.String())
			}
			p.MetaSetMut("http_server_tls_cipher_suite", tls.CipherSuiteName(r.TLS.CipherSuite))
		}
		for k, v := range r.Header {
			if len(v) > 0 {
				p.MetaSetMut(k, v[0])
			}
		}
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				p.MetaSetMut(k, v[0])
			}
		}
		for k, v := range mux.Vars(r) {
			p.MetaSetMut(k, v)
		}
		for _, c := range r.Cookies() {
			p.MetaSetMut(c.Name, c.Value)
		}
	}

	return batch, nil
}

func (ri *Input) deliverHandler(w http.ResponseWriter, r *http.Request) {
	if ri.shutSig.IsSoftStopSignalled() {
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	defer r.Body.Close()

	if ri.conf.RateLimit != "" {
		var tUntil time.Duration
		var err error

		if rerr := ri.mgr.AccessRateLimit(r.Context(), ri.conf.RateLimit, func(rl service.RateLimit) {
			tUntil, err = rl.Access(r.Context())
		}); rerr != nil {
			http.Error(w, "Server error", http.StatusBadGateway)
			ri.log.With("error", rerr).Warn("Failed to access rate limit")
			return
		}
		if err != nil {
			http.Error(w, "Server error", http.StatusBadGateway)
			ri.log.With("error", err).Warn("Failed to access rate limit")
			return
		} else if tUntil > 0 {
			w.Header().Add("Retry-After", strconv.Itoa(int(tUntil.Seconds())))
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
	}

	batch, err := extractBatchFromRequest(r)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		ri.log.With("error", err).Warn("Request read failed")
		return
	}

	batch, store := batch.WithSyncResponseStore()

	ri.log.With("batch_size", len(batch), "path", ri.conf.Path).Trace("Consumed messages from POST")

	resChan := make(chan error, 1)
	select {
	case ri.batches <- batchAndAck{
		batch: batch,
		aFn: func(ctx context.Context, err error) error {
			select {
			case resChan <- err:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
	}:
	case <-r.Context().Done():
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-ri.shutSig.SoftStopChan():
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	select {
	case res, open := <-resChan:
		if !open {
			http.Error(w, "Server closing", http.StatusServiceUnavailable)
			return
		} else if res != nil {
			http.Error(w, res.Error(), http.StatusBadGateway)
			return
		}
	case <-r.Context().Done():
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-ri.shutSig.HardStopChan():
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	var svcBatch service.MessageBatch
	for _, resBatch := range store.Read() {
		svcBatch = append(svcBatch, resBatch...)
	}
	if len(svcBatch) > 0 {
		for k, v := range ri.conf.Response.Headers {
			headerStr, err := svcBatch.TryInterpolatedString(0, v)
			if err != nil {
				ri.log.With("error", err, "header", k).Error("Interpolation of response header error")
				continue
			}
			w.Header().Set(k, headerStr)
		}

		statusCode := 200
		statusCodeStr, err := svcBatch.TryInterpolatedString(0, ri.conf.Response.Status)
		if err != nil {
			ri.log.With("error", err).Error("Interpolation of response status code error")
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if statusCodeStr != "200" {
			if statusCode, err = strconv.Atoi(statusCodeStr); err != nil {
				ri.log.With("error", err).Error("Failed to parse sync response status code expression")
				w.WriteHeader(http.StatusBadGateway)
				return
			}
		}

		if plen := len(svcBatch); plen == 1 {
			part := svcBatch[0]
			_ = ri.conf.Response.ExtractMetadata.Walk(part, func(k, v string) error {
				w.Header().Set(k, v)
				return nil
			})
			payload, err := part.AsBytes()
			if err != nil {
				ri.log.With("error", err).Error("Failed to extract message bytes for sync response")
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", http.DetectContentType(payload))
			}
			w.WriteHeader(statusCode)
			_, _ = w.Write(payload)
		} else if plen > 1 {
			customContentType, customContentTypeExists := ri.conf.Response.Headers["content-type"]

			var buf bytes.Buffer
			writer := multipart.NewWriter(&buf)

			var merr error
			for i := 0; i < plen && merr == nil; i++ {
				part := svcBatch[i]
				_ = ri.conf.Response.ExtractMetadata.Walk(part, func(k, v string) error {
					w.Header().Set(k, v)
					return nil
				})
				payload, err := part.AsBytes()
				if err != nil {
					ri.log.With("error", err).Error("Failed to extract message bytes for sync response")
					continue
				}

				mimeHeader := textproto.MIMEHeader{}
				if customContentTypeExists {
					contentTypeStr, err := svcBatch.TryInterpolatedString(i, customContentType)
					if err != nil {
						ri.log.With("error", err).Error("Interpolation of content-type header error")
						mimeHeader.Set("Content-Type", http.DetectContentType(payload))
					} else {
						mimeHeader.Set("Content-Type", contentTypeStr)
					}
				} else {
					mimeHeader.Set("Content-Type", http.DetectContentType(payload))
				}

				var partWriter io.Writer
				if partWriter, merr = writer.CreatePart(mimeHeader); merr == nil {
					_, merr = io.Copy(partWriter, bytes.NewReader(payload))
				}
			}

			merr = writer.Close()
			if merr == nil {
				w.Header().Del("Content-Type")
				w.Header().Add("Content-Type", writer.FormDataContentType())
				w.WriteHeader(statusCode)
				_, _ = buf.WriteTo(w)
			} else {
				ri.log.With("error", merr).Error("Failed to return sync response")
				w.WriteHeader(http.StatusBadGateway)
			}
		}
	}
}

// Close attempts to stop any further ingestion of data and stops the HTTP
// server.
func (ri *Input) Close(ctx context.Context) error {
	ri.shutSig.TriggerSoftStop()
	defer ri.shutSig.TriggerHardStop()

	return errors.Join(ri.server.Shutdown(ctx), ri.authzPolicy.Close())
}

//------------------------------------------------------------------------------

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

// WriteHeader deletes any Content-Length before freezing headers. The
// Content-Length was set for the uncompressed payload and is wrong after gzip.
// Removing it lets Go's HTTP server use Transfer-Encoding: chunked instead.
//
// All current callers (deliverHandler) call WriteHeader explicitly before
// Write, so this is the primary deletion site. Write also deletes it
// defensively for any future caller that skips an explicit WriteHeader.
func (w gzipResponseWriter) WriteHeader(code int) {
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(code)
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	if w.Header().Get("Content-Type") == "" {
		// If no content type, apply sniffing algorithm to un-gzipped body.
		w.Header().Set("Content-Type", http.DetectContentType(b))
	}
	// Defensive: if Write is called without an explicit WriteHeader, Go's
	// implicit WriteHeader(200) fires on the underlying ResponseWriter
	// directly, bypassing our override. Delete Content-Length here too so
	// it is gone before the implicit header flush.
	w.Header().Del("Content-Length")
	return w.Writer.Write(b)
}

func gzipHandler(hdlr http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			hdlr.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzr := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		hdlr.ServeHTTP(gzr, r)
	})
}
