// Copyright 2024 Redpanda Data, Inc.
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

// client.go implements low-level interactions with the Salesforce REST API.
// It defines the base API path, provides a helper for making authenticated Salesforce API requests with retry
// and error handling, and exposes utilities for retrieving custom fields.
//
// These functions are primarily used by the Salesforce processor when preparing
// queries and resolving custom field identifiers.

package salesforcehttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp/http_metrics"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// salesforceAPIBasePath is the base path for Salesforce Rest API
const salesforceAPIBasePath = "/services"

// This is the general function that calls Salesforce API on a specific URL using the URL object.
// It applies standard header parameters to all calls, Authorization, User-Agent and Accept.
// It uses the helper functions to check against possible response codes and handling the retry-after mechanism
func (s *Client) callSalesforceAPI(ctx context.Context, u *url.URL) ([]byte, error) {
	s.log.Debugf("API call: %s", u.String())

	if s.bearerToken == "" {
		err := s.updateAndSetBearerToken(ctx)
		if err != nil {
			return nil, err
		}
	}

	body, err := s.doSalesforceRequest(ctx, u)
	if err == nil {
		return body, nil
	}

	// Check if it's an HTTPError
	httpErr, ok := err.(*HTTPError)
	if !ok {
		return nil, err
	}

	// Only refresh on 401
	if httpErr.StatusCode != http.StatusUnauthorized {
		return nil, err
	}

	s.log.Warn("Salesforce token expired, refreshing token...")
	// Refresh token
	if err := s.updateAndSetBearerToken(ctx); err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	// Retry once
	retryBody, retryErr := s.doSalesforceRequest(ctx, u)

	if retryErr != nil {
		return nil, fmt.Errorf("request failed: %v", retryErr)
	}

	return retryBody, nil
}

func (s *Client) doSalesforceRequest(ctx context.Context, u *url.URL) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Redpanda-Connect")
	req.Header.Set("Authorization", "Bearer "+s.bearerToken)

	return DoRequestWithRetries(ctx, s.httpClient, req, s.retryOpts)
}

// Function to get the Bearer token from Salesforce Oauth2.0 endpoint using client credentials grant type along with client id and client secret
func (s *Client) updateAndSetBearerToken(ctx context.Context) error {
	apiUrl, err := url.Parse(s.orgURL + salesforceAPIBasePath + "/oauth2/token")
	if err != nil {
		return fmt.Errorf("invalid URL: %v", err)
	}

	query := apiUrl.Query()
	query.Set("grant_type", "client_credentials")
	query.Set("client_id", s.clientId)
	query.Set("client_secret", s.clientSecret)
	apiUrl.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, "POST", apiUrl.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Redpanda-Connect")

	body, err := DoRequestWithRetries(ctx, s.httpClient, req, s.retryOpts)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}

	var result SalesforceAuthResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("cannot map response to custom field struct: %w", err)
	}
	s.bearerToken = result.AccessToken

	return nil
}

// GetAvailableResources function to call get available resources endpoint
// Used mainly for auth testing purposes
func (s *Client) GetAvailableResources(ctx context.Context) ([]byte, error) {
	apiUrl, err := url.Parse(s.orgURL + salesforceAPIBasePath + "/data/" + s.apiVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	body, err := s.callSalesforceAPI(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// GetAllSObjectResources function to get available sObjects endpoint
func (s *Client) GetAllSObjectResources(ctx context.Context) ([]byte, error) {
	apiUrl, err := url.Parse(s.orgURL + salesforceAPIBasePath + "/data/" + s.apiVersion + "/sobjects")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	body, err := s.callSalesforceAPI(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// GetSObjectResource function to call receive the description of the sObject
func (s *Client) GetSObjectResource(ctx context.Context, sObj string) ([]byte, error) {
	apiUrl, err := url.Parse(s.orgURL + salesforceAPIBasePath + "/data/" + s.apiVersion + "/sobjects/" + sObj + "/describe")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	body, err := s.callSalesforceAPI(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// GetSObjectData function to call receive the description of the sObject
func (s *Client) GetSObjectData(ctx context.Context, query string) ([]byte, error) {
	apiUrl, err := url.Parse(s.orgURL + salesforceAPIBasePath + "/data/" + s.apiVersion + "/query?q=" + query)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}

	body, err := s.callSalesforceAPI(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// Client is the implementation of Salesforce API queries. It holds the client state and orchestrates calls into the salesforcehttp package.
type Client struct {
	orgURL       string
	clientId     string
	clientSecret string
	apiVersion   string
	bearerToken  string
	httpClient   *http.Client
	retryOpts    RetryOptions
	log          *service.Logger
}

// NewClient is the constructor for a Client object
func NewClient(log *service.Logger, orgUrl, clientId, clientSecret, apiVersion string, maxRetries int, metrics *service.Metrics, httpClient *http.Client) (*Client, error) {
	return &Client{
		log:          log,
		orgURL:       orgUrl,
		clientId:     clientId,
		clientSecret: clientSecret,
		apiVersion:   apiVersion,
		retryOpts: RetryOptions{
			MaxRetries: maxRetries,
		},
		httpClient: http_metrics.NewInstrumentedClient(
			metrics, "salesforce_http",
			httpClient),
		bearerToken: "",
	}, nil
}
