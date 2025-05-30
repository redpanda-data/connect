// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/auth0/go-jwt-middleware/v2/jwks"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/twmb/go-cache/cache"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	rpEnvJWTIssuer   = "REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL"
	rpEnvJWTAudience = "REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE"
	rpEnvJWTOrgID    = "REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID"
)

// RPJWTMiddleware implements a custom JWT validation for the RP platform that
// ensures a given request matches a specified organisation and audience.
type RPJWTMiddleware struct {
	logger       *service.Logger
	jwtValidator *validator.Validator
	orgID        string

	validationCache *cache.Cache[string, *validator.ValidatedClaims]
}

// NewRPJWTMiddleware creates a new RP JWT middleware.
func NewRPJWTMiddleware(log *service.Logger) (*RPJWTMiddleware, error) {
	issuerURLStr := os.Getenv(rpEnvJWTIssuer)
	if issuerURLStr == "" {
		return nil, nil
	}

	audience := os.Getenv(rpEnvJWTAudience)
	if audience == "" {
		return nil, fmt.Errorf("gateway JWT authentication requires an audience set via %v", rpEnvJWTAudience)
	}

	orgID := os.Getenv(rpEnvJWTOrgID)
	if orgID == "" {
		return nil, fmt.Errorf("gateway JWT authentication requires an organisation ID set via %v", rpEnvJWTOrgID)
	}

	issuerURL, err := url.Parse(issuerURLStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse gateway JWT issuer URL: %w", err)
	}

	provider := jwks.NewCachingProvider(issuerURL, time.Minute)

	jwtValidator, err := validator.New(
		provider.KeyFunc,
		validator.RS256,
		issuerURL.String(),
		[]string{audience},
		validator.WithAllowedClockSkew(time.Minute),
		validator.WithCustomClaims(
			func() validator.CustomClaims {
				return &rpCustomClaims{}
			},
		),
	)
	if err != nil {
		return nil, errors.New("failed to set up the jwt validator")
	}

	return &RPJWTMiddleware{
		logger:       log,
		jwtValidator: jwtValidator,
		orgID:        orgID,

		validationCache: cache.New[string, *validator.ValidatedClaims](cache.MaxAge(10*time.Second), cache.MaxErrorAge(time.Second)),
	}, nil
}

type rpCustomClaims struct {
	OrgID string `json:"https://cloud.redpanda.com/organization_id,omitempty"`
}

func (r *rpCustomClaims) Validate(_ context.Context) error {
	if r.OrgID == "" {
		return errors.New("there is no organization present in the token")
	}
	return nil
}

// Wrap a handler with JWT validation. Any request that fails validation will
// be rejected and next will not be called.
func (r *RPJWTMiddleware) Wrap(next http.Handler) http.Handler {
	if r == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		authToken, err := extractAuthenticationToken(req)
		if err != nil || authToken == "" {
			r.logger.With("error", err).Error("Authentication token not found")
			http.Error(w, "authentication token not found", http.StatusBadRequest)
			return
		}

		var claims *validator.ValidatedClaims
		if claims, err = r.validateToken(req.Context(), authToken); err != nil {
			r.logger.With("error", err).Error("Authentication token was not valid")
			http.Error(w, "authentication token was invalid", http.StatusBadRequest)
			return
		}

		customClaims, ok := claims.CustomClaims.(*rpCustomClaims)
		if !ok {
			r.logger.Error("Failed to extract custom claims")
			http.Error(w, "authentication claims were not found", http.StatusBadRequest)
			return
		}

		if customClaims.OrgID != r.orgID {
			r.logger.With("org_id", customClaims.OrgID).Error("Organisation ID mismatch")
			http.Error(w, "organisation mismatch", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, req)
	})
}

func extractAuthenticationToken(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", nil
	}

	authHeaderParts := strings.Fields(authHeader)
	if len(authHeaderParts) != 2 || !strings.EqualFold(authHeaderParts[0], "bearer") {
		return "", errors.New("authorization header format must be Bearer {token}")
	}

	return authHeaderParts[1], nil
}

func (r *RPJWTMiddleware) validateToken(ctx context.Context, tokenString string) (*validator.ValidatedClaims, error) {
	parsedToken, err, _ := r.validationCache.Get(tokenString, func() (*validator.ValidatedClaims, error) {
		parsedToken, err := r.jwtValidator.ValidateToken(ctx, tokenString)
		if err != nil {
			return nil, err
		}

		validatedClaims, ok := (parsedToken).(*validator.ValidatedClaims)
		if !ok {
			return nil, errors.New("invalid claims type")
		}

		return validatedClaims, nil
	})

	return parsedToken, err
}
