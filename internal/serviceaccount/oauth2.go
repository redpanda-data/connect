// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package serviceaccount

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

var (
	globalConfigMu sync.RWMutex
	globalConfig   *oauth2Config
)

// oauth2Config holds OAuth2 client credentials configuration.
type oauth2Config struct {
	tokenURL     string
	audience     string
	clientID     string
	clientSecret string
	tokenSource  oauth2.TokenSource
	httpClient   *http.Client
}

// InitGlobal initializes the global service account OAuth2 configuration.
// This should be called once during application startup, typically from CLI flag parsing.
func InitGlobal(ctx context.Context, tokenURL, clientID, clientSecret, audience string) error {
	if tokenURL == "" || clientID == "" || clientSecret == "" {
		return errors.New("tokenURL, clientID, and clientSecret are required")
	}

	config := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
		Scopes:       []string{},
	}

	// Add audience parameter if provided
	if audience != "" {
		config.EndpointParams = map[string][]string{
			"audience": {audience},
		}
	}

	tokenSource := config.TokenSource(ctx)

	// Test token acquisition to fail fast if auth is misconfigured
	if _, err := tokenSource.Token(); err != nil {
		return fmt.Errorf("failed to acquire OAuth2 token: %w", err)
	}

	globalConfigMu.Lock()
	defer globalConfigMu.Unlock()

	globalConfig = &oauth2Config{
		tokenURL:     tokenURL,
		audience:     audience,
		clientID:     clientID,
		clientSecret: clientSecret,
		tokenSource:  tokenSource,
		httpClient:   config.Client(ctx),
	}

	return nil
}

// GetTokenSource returns the global OAuth2 token source.
// Returns an error if service account authentication has not been initialized.
func GetTokenSource() (oauth2.TokenSource, error) {
	globalConfigMu.RLock()
	defer globalConfigMu.RUnlock()

	if globalConfig == nil {
		return nil, errors.New("service account authentication has not been set up")
	}

	return globalConfig.tokenSource, nil
}

// GetHTTPClient returns an HTTP client configured with OAuth2 authentication.
// Returns an error if service account authentication has not been initialized.
func GetHTTPClient() (*http.Client, error) {
	globalConfigMu.RLock()
	defer globalConfigMu.RUnlock()

	if globalConfig == nil {
		return nil, errors.New("service account authentication has not been set up")
	}

	return globalConfig.httpClient, nil
}
