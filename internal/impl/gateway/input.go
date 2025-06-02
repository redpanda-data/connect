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

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/klauspost/compress/gzip"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/gateway"
)

const (
	hsiFieldPath                    = "path"
	hsiFieldRateLimit               = "rate_limit"
	hsiFieldResponse                = "sync_response"
	hsiFieldResponseStatus          = "status"
	hsiFieldResponseHeaders         = "headers"
	hsiFieldResponseExtractMetadata = "metadata_headers"
)

type hsiConfig struct {
	Path      string
	RateLimit string
	Response  hsiResponseConfig

	// Set via environment variables
	Address string
	CORS    corsConfig
}

type corsConfig struct {
	enabled        bool
	allowedOrigins []string
}

func (conf corsConfig) WrapHandler(handler http.Handler) http.Handler {
	if !conf.enabled {
		return handler
	}
	return handlers.CORS(
		handlers.AllowedOrigins(conf.allowedOrigins),
		handlers.AllowedHeaders([]string{"Content-Type", "Authorization"}),
		handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}),
	)(handler)
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
	return
}

const (
	rpEnvAddress     = "REDPANDA_CLOUD_GATEWAY_ADDRESS"
	rpEnvCorsOrigins = "REDPANDA_CLOUD_GATEWAY_CORS_ORIGINS"
)

func (h *hsiConfig) applyEnvVarOverrides() error {
	if h.Address = os.Getenv(rpEnvAddress); h.Address == "" {
		return errors.New("an address must be specified via env var for this input to be functional")
	}

	if v := os.Getenv(rpEnvCorsOrigins); v != "" {
		h.CORS.enabled = true
		h.CORS.allowedOrigins = strings.Split(v, ",")
		for i, o := range h.CORS.allowedOrigins {
			h.CORS.allowedOrigins[i] = strings.TrimSpace(o)
		}
	}

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

You can access these metadata fields using xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].`).
		Fields(
			service.NewStringField(hsiFieldPath).
				Description("The endpoint path to listen for data delivery requests.").
				Default("/"),
			service.NewStringField(hsiFieldRateLimit).
				Description("An optional xref:components:rate_limits/about.adoc[rate limit] to throttle requests by.").
				Default(""),
			service.NewObjectField(hsiFieldResponse,
				service.NewInterpolatedStringField(hsiFieldResponseStatus).
					Description("Specify the status code to return with synchronous responses. This is a string value, which allows you to customize it based on resulting payloads and their metadata.").
					Examples(`${! json("status") }`, `${! meta("status") }`).
					Default("200"),
				service.NewInterpolatedStringMapField(hsiFieldResponseHeaders).
					Description("Specify headers to return with synchronous responses.").
					Default(map[string]any{
						"Content-Type": "application/octet-stream",
					}),
				service.NewMetadataFilterField(hsiFieldResponseExtractMetadata).
					Description("Specify criteria for which metadata values are added to the response as headers."),
			).
				Description("Customize messages returned via xref:guides:sync_responses.adoc[synchronous responses].").
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

	mux    *mux.Router
	server *http.Server

	rpJWTValidator *gateway.RPJWTMiddleware

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
	if h.rpJWTValidator, err = gateway.NewRPJWTMiddleware(mgr); err != nil {
		return nil, err
	}

	if h.conf.RateLimit != "" {
		if !h.mgr.HasRateLimit(h.conf.RateLimit) {
			return nil, fmt.Errorf("rate limit resource '%v' was not found", h.conf.RateLimit)
		}
	}

	return &h, nil
}

//------------------------------------------------------------------------------

func (ri *Input) createHandler() (h http.Handler) {
	h = http.HandlerFunc(ri.deliverHandler)
	h = gzipHandler(h)
	h = ri.rpJWTValidator.Wrap(h)
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

	ri.server = &http.Server{Addr: ri.conf.Address, Handler: ri.mux}

	go func() {
		defer ri.shutSig.TriggerHasStopped()

		ri.log.With("address", ri.conf.Address+ri.conf.Path).Info("Receiving HTTP messages")
		if err := ri.server.ListenAndServe(); err != http.ErrServerClosed {
			ri.log.With("error").Error("Server error")
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
		return nil, fmt.Errorf("failed to parse media type: %w", err)
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			var p *multipart.Part
			if p, err = mr.NextPart(); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, fmt.Errorf("failed to obtain next multipart message part: %w", err)
			}
			var msgBytes []byte
			if msgBytes, err = io.ReadAll(p); err != nil {
				return nil, fmt.Errorf("failed to read multipart message part: %w", err)
			}
			batch = append(batch, service.NewMessage(msgBytes))
		}
	} else {
		var msgBytes []byte
		if msgBytes, err = io.ReadAll(r.Body); err != nil {
			return nil, fmt.Errorf("failed to read body: %w", err)
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

	if ri.server == nil {
		return nil
	}
	return ri.server.Shutdown(ctx)
}

//------------------------------------------------------------------------------

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	if w.Header().Get("Content-Type") == "" {
		// If no content type, apply sniffing algorithm to un-gzipped body.
		w.Header().Set("Content-Type", http.DetectContentType(b))
	}
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
