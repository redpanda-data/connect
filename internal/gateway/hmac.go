// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// hmacHashConstructors maps supported algorithm names to their hash
// constructors.
var hmacHashConstructors = map[string]func() hash.Hash{
	"sha256": sha256.New,
	"sha512": sha512.New,
}

// HMACMiddleware authenticates incoming requests by verifying an HMAC
// signature of the raw request body against a shared secret. This is used
// to validate webhook-style callers, such as Terraform Cloud Run Tasks,
// that sign their payloads instead of presenting a bearer token. An
// optional fixed prefix on the signature header value (e.g. GitHub's
// `sha256=`) can be stripped prior to hex decoding; it has no bearing on
// which algorithm is used to verify the signature, which always comes from
// configuration.
type HMACMiddleware struct {
	secret      []byte
	header      string
	prefix      string
	newHash     func() hash.Hash
	macSize     int
	logger      *service.Logger
	maxBodySize int64
}

// HMACConfig configures an HMACMiddleware.
type HMACConfig struct {
	// Secret is the shared secret key used to compute and verify the HMAC
	// signature.
	Secret string
	// Header is the name of the request header containing the HMAC
	// signature of the request body.
	Header string
	// Algorithm is the hash algorithm used to compute the HMAC signature.
	Algorithm string
	// Prefix is an optional exact-match prefix that the signature header
	// value must begin with; it is stripped before the remaining value is
	// hex-decoded. It does not influence which algorithm is used for
	// verification.
	Prefix string
	// MaxBodySize is the maximum size of the request body in bytes that
	// will be read and buffered for signature verification.
	MaxBodySize int
}

// NewHMACMiddleware creates a new HMAC signature validation middleware.
func NewHMACMiddleware(mgr *service.Resources, conf HMACConfig) (*HMACMiddleware, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, fmt.Errorf("gateway hmac auth requires a valid license: %w", err)
	}

	if conf.Secret == "" {
		return nil, errors.New("gateway HMAC authentication requires a non-empty secret")
	}
	if conf.Header == "" {
		return nil, errors.New("gateway HMAC authentication requires a non-empty header name")
	}
	if conf.MaxBodySize <= 0 {
		return nil, errors.New("gateway HMAC authentication requires a positive max body size")
	}

	newHash, exists := hmacHashConstructors[conf.Algorithm]
	if !exists {
		supported := strings.Join(slices.Sorted(maps.Keys(hmacHashConstructors)), ", ")
		return nil, fmt.Errorf("gateway HMAC authentication algorithm %q is not supported, valid options are: %s", conf.Algorithm, supported)
	}

	return &HMACMiddleware{
		secret:      []byte(conf.Secret),
		header:      conf.Header,
		prefix:      conf.Prefix,
		newHash:     newHash,
		macSize:     newHash().Size(),
		logger:      mgr.Logger(),
		maxBodySize: int64(conf.MaxBodySize),
	}, nil
}

// Wrap a handler with HMAC signature validation. Any request that fails
// validation will be rejected and next will not be called.
func (m *HMACMiddleware) Wrap(next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		signatureHex := req.Header.Get(m.header)
		if signatureHex == "" {
			m.logger.With("header", m.header).Debug("Signature header not found")
			http.Error(w, "signature header not found", http.StatusUnauthorized)
			return
		}

		signatureHex, ok := strings.CutPrefix(signatureHex, m.prefix)
		if !ok {
			m.logger.Debug("Signature prefix mismatch")
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		}

		signature, err := hex.DecodeString(signatureHex)
		if err != nil {
			m.logger.With("error", err).Debug("Signature header is not valid hex")
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		}

		// Length is public information (no timing concern in comparing it),
		// and rejecting early here avoids buffering the request body for a
		// signature that can never match.
		if len(signature) != m.macSize {
			m.logger.With("expected_bytes", m.macSize, "actual_bytes", len(signature)).Debug("Signature length mismatch")
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		}

		req.Body = http.MaxBytesReader(w, req.Body, m.maxBodySize)

		body, err := io.ReadAll(req.Body)
		if err != nil {
			if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
				m.logger.With("error", err).Debug("Request body exceeded maximum size for signature verification")
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			m.logger.With("error", err).Error("Failed to read request body for signature verification")
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))

		mac := hmac.New(m.newHash, m.secret)
		mac.Write(body)
		expected := mac.Sum(nil)

		if !hmac.Equal(signature, expected) {
			m.logger.Debug("HMAC signature verification failed")
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, req)
	})
}
