// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package a2a

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	// RPEnvTokenURL is the OAuth2 token endpoint URL for client credentials grant.
	RPEnvTokenURL = "REDPANDA_CLOUD_TOKEN_URL"
	// RPEnvAudience is the OAuth2 audience parameter.
	RPEnvAudience = "REDPANDA_CLOUD_AUDIENCE"
	// RPEnvClientID is the OAuth2 client ID for service account authentication.
	RPEnvClientID = "REDPANDA_CLOUD_CLIENT_ID"
	// RPEnvClientSecret is the OAuth2 client secret for service account authentication.
	RPEnvClientSecret = "REDPANDA_CLOUD_CLIENT_SECRET"
)

// OAuth2Config holds OAuth2 client credentials configuration from environment variables.
type OAuth2Config struct {
	TokenURL     string
	Audience     string
	ClientID     string
	ClientSecret string
}

// NewOAuth2ConfigFromEnv creates OAuth2 configuration from environment variables.
// Returns nil if required env vars are not set (allowing processor to work without auth).
func NewOAuth2ConfigFromEnv() (*OAuth2Config, error) {
	tokenURL := os.Getenv(RPEnvTokenURL)
	clientID := os.Getenv(RPEnvClientID)
	clientSecret := os.Getenv(RPEnvClientSecret)
	audience := os.Getenv(RPEnvAudience)

	// Check each required field individually and report which ones are missing
	var missing []string
	if tokenURL == "" {
		missing = append(missing, RPEnvTokenURL)
	}
	if clientID == "" {
		missing = append(missing, RPEnvClientID)
	}
	if clientSecret == "" {
		missing = append(missing, RPEnvClientSecret)
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("OAuth2 configuration incomplete, missing environment variables: %v", missing)
	}

	return &OAuth2Config{
		TokenURL:     tokenURL,
		Audience:     audience,
		ClientID:     clientID,
		ClientSecret: clientSecret,
	}, nil
}

// CreateHTTPClient creates an HTTP client with OAuth2 client credentials authentication.
func (c *OAuth2Config) CreateHTTPClient(ctx context.Context) (*http.Client, oauth2.TokenSource, error) {
	if c == nil {
		return http.DefaultClient, nil, nil
	}

	config := &clientcredentials.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		TokenURL:     c.TokenURL,
		Scopes:       []string{},
	}

	// Add audience parameter if provided
	if c.Audience != "" {
		config.EndpointParams = map[string][]string{
			"audience": {c.Audience},
		}
	}

	tokenSource := config.TokenSource(ctx)

	// Test token acquisition to fail fast if auth is misconfigured
	if _, err := tokenSource.Token(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire OAuth2 token: %w", err)
	}

	return config.Client(ctx), tokenSource, nil
}
