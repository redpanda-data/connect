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
	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	rpEnvJWTIssuer   = "REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL"
	rpEnvJWTAudience = "REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE"
	rpEnvJWTOrgID    = "REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID"
)

// jwtValidator contains the JWT validation logic and is technology-agnostic.
type jwtValidator struct {
	orgID     string
	validator *validator.Validator
	cache     *cache.Cache[string, *validator.ValidatedClaims]
}

func newJWTValidator(mgr *service.Resources) (*jwtValidator, error) {
	issuerURLStr := os.Getenv(rpEnvJWTIssuer)
	if issuerURLStr == "" {
		return nil, nil
	}

	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, fmt.Errorf("gateway jwt auth requires a valid license: %w", err)
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

	v, err := validator.New(
		jwks.NewCachingProvider(issuerURL, time.Minute).KeyFunc,
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

	return &jwtValidator{
		orgID:     orgID,
		validator: v,
		cache:     cache.New[string, *validator.ValidatedClaims](cache.MaxAge(10*time.Second), cache.MaxErrorAge(time.Second)),
	}, nil
}

func (r *jwtValidator) validateToken(ctx context.Context, tokenString string) (*validator.ValidatedClaims, error) {
	c, err, _ := r.cache.Get(tokenString, func() (*validator.ValidatedClaims, error) {
		token, err := r.validator.ValidateToken(ctx, tokenString)
		if err != nil {
			return nil, err
		}

		c, ok := (token).(*validator.ValidatedClaims)
		if !ok {
			return nil, errors.New("invalid claims type")
		}
		return c, nil
	})

	return c, err
}

// validateAndGetPrincipal validates token and extracts principal.
func (r *jwtValidator) validateAndGetPrincipal(ctx context.Context, token string) (authz.PrincipalID, error) {
	c, err := r.validateToken(ctx, token)
	if err != nil {
		return "", err
	}
	cc, ok := c.CustomClaims.(*rpCustomClaims)
	if !ok {
		return "", errors.New("authentication claims were not found")
	}

	if cc.OrgID != r.orgID {
		return "", errors.New("organisation mismatch")
	}

	if cc.AccountInfo.Email == "" {
		return "", errors.New("missing email claim")
	}

	return authz.PrincipalID("User:" + cc.AccountInfo.Email), nil
}

type rpCustomClaims struct {
	OrgID       string `json:"https://cloud.redpanda.com/organization_id,omitempty"`
	AccountInfo struct {
		Email string `json:"email,omitempty"`
	} `json:"account_info"`
}

func (r *rpCustomClaims) Validate(_ context.Context) error {
	if r.OrgID == "" {
		return errors.New("there is no organization present in the token")
	}
	if r.AccountInfo.Email == "" {
		return errors.New("there is no email present in the token")
	}
	return nil
}

// RPJWTMiddleware implements a custom JWT validation for the Redpanda platform
// that ensures a given request matches a specified organization and audience.
type RPJWTMiddleware struct {
	jwt    *jwtValidator
	logger *service.Logger
}

// NewRPJWTMiddleware creates a new RP JWT middleware.
func NewRPJWTMiddleware(mgr *service.Resources) (*RPJWTMiddleware, error) {
	jwt, err := newJWTValidator(mgr)
	if err != nil {
		return nil, err
	}
	if jwt == nil {
		return nil, nil
	}
	return &RPJWTMiddleware{
		jwt:    jwt,
		logger: mgr.Logger(),
	}, nil
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

		principal, err := r.jwt.validateAndGetPrincipal(req.Context(), authToken)
		if err != nil {
			r.logger.With("error", err).Error("Authentication failed")
			http.Error(w, "authentication failed", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, req.WithContext(ContextWithValidatedPrincipalID(req.Context(), principal)))
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

type validatedPrincipalIDContextKeyType string

const validatedPrincipalIDContextKey validatedPrincipalIDContextKeyType = ""

// ContextWithValidatedPrincipalID adds a validated principal to an existing [context.Context].
func ContextWithValidatedPrincipalID(ctx context.Context, principal authz.PrincipalID) context.Context {
	return context.WithValue(ctx, validatedPrincipalIDContextKey, principal)
}

// ValidatedPrincipalIDFromContext extracts a validated principal from the context, if present.
func ValidatedPrincipalIDFromContext(ctx context.Context) (authz.PrincipalID, bool) {
	pid, ok := ctx.Value(validatedPrincipalIDContextKey).(authz.PrincipalID)
	return pid, ok
}
