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

// client.go implements low-level interactions with the Salesforce REST API.
// It defines the base API path, provides a helper for making authenticated Salesforce API requests with retry
// and error handling, and exposes utilities for retrieving custom fields.
//
// These functions are primarily used by the Salesforce processor when preparing
// queries and resolving custom field identifiers.

package salesforcehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp/metrics"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// salesforceAPIBasePath is the base path for Salesforce Rest API
const salesforceAPIBasePath = "/services"

// This is the general function that calls Salesforce API on a specific URL using the URL object.
// It applies standard header parameters to all calls, Authorization, User-Agent and Accept.
// It uses the helper functions to check against possible response codes and handling the retry-after mechanism
func (s *Client) callSalesforceAPI(ctx context.Context, u *url.URL) ([]byte, error) {
	s.log.Debugf("API call: %s", u.String())

	if s.getBearerToken() == "" {
		if err := s.updateAndSetBearerToken(ctx); err != nil {
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
		return nil, fmt.Errorf("request failed: %w", retryErr)
	}

	return retryBody, nil
}

func (s *Client) doSalesforceRequest(ctx context.Context, u *url.URL) ([]byte, error) {
	newReq := func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Redpanda-Connect")
		req.Header.Set("Authorization", "Bearer "+s.getBearerToken())
		return req, nil
	}

	return DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
}

// Function to get the Bearer token from Salesforce Oauth2.0 endpoint using client credentials grant type along with client id and client secret
func (s *Client) updateAndSetBearerToken(ctx context.Context) error {
	apiUrl, err := url.Parse(s.orgURL + salesforceAPIBasePath + "/oauth2/token")
	if err != nil {
		return fmt.Errorf("invalid token endpoint URL: %w", err)
	}

	// Build form-encoded body
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", s.clientID)
	form.Set("client_secret", s.clientSecret)

	encodedForm := form.Encode()
	newReq := func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", apiUrl.String(), strings.NewReader(encodedForm))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Redpanda-Connect")
		return req, nil
	}

	body, err := DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	var result SalesforceAuthResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("cannot map response to custom field struct: %w", err)
	}
	s.bearerToken.Store(result.AccessToken)
	s.instanceURL = result.InstanceUrl

	// Extract org ID from the identity URL: https://login.salesforce.com/id/{orgId}/{userId}
	if result.Id != "" {
		parts := strings.Split(strings.TrimRight(result.Id, "/"), "/")
		if len(parts) >= 2 {
			s.tenantID = parts[len(parts)-2]
		}
	}

	return nil
}

// GetAvailableResources function to call get available resources endpoint
// Used mainly for auth testing purposes
func (s *Client) GetAvailableResources(ctx context.Context) ([]byte, error) {
	apiUrl, err := url.Parse(s.orgURL + salesforceAPIBasePath + "/data/" + s.apiVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
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
		return nil, fmt.Errorf("invalid URL: %w", err)
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
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	body, err := s.callSalesforceAPI(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// GetSObjectData function to call receive the description of the sObject
func (s *Client) GetSObjectData(ctx context.Context, query string) ([]byte, error) {
	apiUrl, err := url.Parse(s.orgURL + salesforceAPIBasePath + "/data/" + s.apiVersion + "/query")
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}
	q := apiUrl.Query()
	q.Set("q", query)
	apiUrl.RawQuery = q.Encode()

	body, err := s.callSalesforceAPI(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// SObjectInfo holds basic info about an SObject
type SObjectInfo struct {
	Name   string
	Fields []string // Cached field names
}

// Cursor represents the current position in the batch processing
type Cursor struct {
	SObjectIndex int    `json:"sobject_index"`
	SObjectName  string `json:"sobject_name"`
	NextURL      string `json:"next_url"` // For pagination within an SObject
}

// Client is the implementation of Salesforce API queries. It holds the client state and orchestrates calls into the salesforcehttp package.
type Client struct {
	orgURL       string
	clientID     string
	clientSecret string
	apiVersion   string
	bearerToken  atomic.Value
	instanceURL  string
	tenantID     string
	httpClient   *http.Client
	retryOpts    RetryOptions
	log          *service.Logger

	// Cache the SObject list to avoid refetching
	sobjectList []SObjectInfo

	// SObjects that returned 400 during data fetch — never retry them
	unsupportedSObjects map[string]struct{}
}

func (s *Client) getBearerToken() string {
	v, _ := s.bearerToken.Load().(string)
	return v
}

// NewClient is the constructor for a Client object
func NewClient(orgURL, clientID, clientSecret, apiVersion string, maxRetries int, httpClient *http.Client, log *service.Logger, m *service.Metrics) (*Client, error) {
	return &Client{
		log:          log,
		orgURL:       orgURL,
		clientID:     clientID,
		clientSecret: clientSecret,
		apiVersion:   apiVersion,
		retryOpts: RetryOptions{
			MaxRetries: maxRetries,
		},
		httpClient:          metrics.NewInstrumentedClient(m, "salesforce_http", httpClient),
		unsupportedSObjects: make(map[string]struct{}),
	}, nil
}

// BearerToken returns the current OAuth bearer token.
func (s *Client) BearerToken() string { return s.getBearerToken() }

// InstanceURL returns the Salesforce instance URL obtained during authentication.
func (s *Client) InstanceURL() string { return s.instanceURL }

// TenantID returns the Salesforce org/tenant ID extracted from the identity URL.
func (s *Client) TenantID() string { return s.tenantID }

// RefreshToken forces a token refresh and returns an error if it fails.
func (s *Client) RefreshToken(ctx context.Context) error {
	return s.updateAndSetBearerToken(ctx)
}

// GraphQL sends a GraphQL query to Salesforce and returns the raw response body.
func (s *Client) GraphQL(ctx context.Context, query string) ([]byte, error) {
	apiUrl, err := url.Parse(s.orgURL + salesforceAPIBasePath + "/data/" + s.apiVersion + "/graphql")
	if err != nil {
		return nil, fmt.Errorf("invalid GraphQL URL: %w", err)
	}

	payload := map[string]string{
		"query": query,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GraphQL payload: %w", err)
	}

	return s.callSalesforceAPIWithBody(ctx, apiUrl, bodyBytes)
}

// RestQueryAll is deprecated - use GetNextBatch instead.
// Kept for backward compatibility.
func (s *Client) RestQueryAll(ctx context.Context) (service.MessageBatch, error) {
	var batch service.MessageBatch

	raw, err := s.GetAllSObjectResources(ctx)
	if err != nil {
		return nil, err
	}

	var list SObjectList
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}

	skipList := map[string]bool{
		"AIUpdateRecordEvent": true,
	}

	for _, sobj := range list.Sobjects {
		if skipList[sobj.Name] {
			continue
		}

		describeJSON, err := s.GetSObjectResource(ctx, sobj.Name)
		if err != nil {
			return nil, err
		}

		fields, err := extractFieldNames(describeJSON)
		if err != nil {
			return nil, err
		}

		soql := buildSOQL(sobj.Name, fields)
		queryResults, err := s.GetSObjectData(ctx, soql)
		if err != nil {
			return nil, err
		}

		var qr QueryResult
		if err := json.Unmarshal(queryResults, &qr); err != nil {
			return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
		}

		msg := service.NewMessage(queryResults)
		msg.MetaSet("sobject", sobj.Name)
		msg.MetaSet("total_size", strconv.Itoa(qr.TotalSize))

		batch = append(batch, msg)
	}

	return batch, nil
}

// RestQueryFiltered fetches data using the provided SOQL query via the REST API.
// Returns one message per record in the result set.
func (s *Client) RestQueryFiltered(ctx context.Context, soql string) (service.MessageBatch, error) {
	raw, err := s.GetSObjectData(ctx, soql)
	if err != nil {
		return nil, err
	}

	var qr QueryResult
	if err := json.Unmarshal(raw, &qr); err != nil {
		return nil, fmt.Errorf("failed to parse query result: %w", err)
	}

	var batch service.MessageBatch
	for _, record := range qr.Records {
		msg := service.NewMessage(record)
		msg.MetaSet("soql", soql)
		msg.MetaSet("total_size", strconv.Itoa(qr.TotalSize))
		batch = append(batch, msg)
	}

	return batch, nil
}

// GraphQLQueryFiltered fetches data using the provided GraphQL query.
func (s *Client) GraphQLQueryFiltered(ctx context.Context, query string) (service.MessageBatch, error) {
	raw, err := s.GraphQL(ctx, query)
	if err != nil {
		return nil, err
	}

	msg := service.NewMessage(raw)
	msg.MetaSet("query", query)

	return service.MessageBatch{msg}, nil
}

// GetNextBatch returns the next batch of records based on the cursor.
// This is the key method for cursor-based batching support.
func (s *Client) GetNextBatch(ctx context.Context, cursor Cursor) (service.MessageBatch, Cursor, bool, error) {
	// Step 1: Ensure we have the SObject list loaded
	if len(s.sobjectList) == 0 {
		if err := s.loadSObjectList(ctx); err != nil {
			return nil, cursor, false, err
		}
	}

	// Step 2: Check if we've exhausted all SObjects
	if cursor.SObjectIndex >= len(s.sobjectList) {
		return nil, cursor, true, nil // done = true
	}

	currentSObject := s.sobjectList[cursor.SObjectIndex]

	var queryResults []byte
	var err error

	// Step 3: Handle pagination within the current SObject
	if cursor.NextURL != "" {
		// Continue fetching from pagination URL
		queryResults, err = s.fetchNextRecordsPage(ctx, cursor.NextURL)
		if err != nil {
			return nil, cursor, false, err
		}
	} else {
		// Skip SObjects that previously returned 400
		if _, unsupported := s.unsupportedSObjects[currentSObject.Name]; unsupported {
			skipCursor := cursor
			skipCursor.SObjectIndex++
			skipCursor.NextURL = ""
			return nil, skipCursor, false, nil
		}

		// First query for this SObject
		soql := buildSOQL(currentSObject.Name, currentSObject.Fields)
		queryResults, err = s.GetSObjectData(ctx, soql)
		if err != nil {
			// 400 means the SObject requires filter criteria or doesn't support unrestricted SOQL.
			// Record it permanently so future cycles don't retry it.
			var httpErr *HTTPError
			if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusBadRequest {
				s.unsupportedSObjects[currentSObject.Name] = struct{}{}
				s.log.Warnf("Skipping SObject %s permanently: query not supported (400): query=%q reason=%s", currentSObject.Name, soql, httpErr.Body)
				skipCursor := cursor
				skipCursor.SObjectIndex++
				skipCursor.NextURL = ""
				return nil, skipCursor, false, nil
			}
			return nil, cursor, false, err
		}
	}

	// Step 4: Parse the result to check for nextRecordsUrl
	var qr QueryResult
	if err := json.Unmarshal(queryResults, &qr); err != nil {
		return nil, cursor, false, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	// Step 5: Update cursor
	nextCursor := cursor
	nextCursor.SObjectName = currentSObject.Name

	if qr.NextRecordsUrl != "" {
		// More pages exist for this SObject
		nextCursor.NextURL = qr.NextRecordsUrl
	} else {
		// Move to next SObject
		nextCursor.SObjectIndex++
		nextCursor.NextURL = ""
	}

	// Step 6: Create message batch — one message per record
	var batch service.MessageBatch
	for _, record := range qr.Records {
		msg := service.NewMessage(record)
		msg.MetaSet("sobject", currentSObject.Name)
		msg.MetaSet("total_size", strconv.Itoa(qr.TotalSize))
		batch = append(batch, msg)
	}

	return batch, nextCursor, false, nil
}

// callSalesforceAPIWithBody sends an authenticated POST request with a JSON body.
func (s *Client) callSalesforceAPIWithBody(ctx context.Context, u *url.URL, body []byte) ([]byte, error) {
	s.log.Debugf("API POST call: %s", u.String())

	if s.getBearerToken() == "" {
		if err := s.updateAndSetBearerToken(ctx); err != nil {
			return nil, err
		}
	}

	resp, err := s.doSalesforcePostRequest(ctx, u, body)
	if err == nil {
		return resp, nil
	}

	httpErr, ok := err.(*HTTPError)
	if !ok {
		return nil, err
	}

	if httpErr.StatusCode != http.StatusUnauthorized {
		return nil, err
	}

	s.log.Warn("Salesforce token expired, refreshing token...")
	if err := s.updateAndSetBearerToken(ctx); err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	retryResp, retryErr := s.doSalesforcePostRequest(ctx, u, body)
	if retryErr != nil {
		return nil, fmt.Errorf("request failed: %w", retryErr)
	}

	return retryResp, nil
}

func (s *Client) doSalesforcePostRequest(ctx context.Context, u *url.URL, body []byte) ([]byte, error) {
	newReq := func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Redpanda-Connect")
		req.Header.Set("Authorization", "Bearer "+s.getBearerToken())
		return req, nil
	}

	return DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
}

func (s *Client) loadSObjectList(ctx context.Context) error {
	raw, err := s.GetAllSObjectResources(ctx)
	if err != nil {
		return err
	}

	var list SObjectList
	if err := json.Unmarshal(raw, &list); err != nil {
		return err
	}

	skipList := map[string]bool{
		"AIUpdateRecordEvent": true,
	}

	s.sobjectList = make([]SObjectInfo, 0)
	for _, sobj := range list.Sobjects {
		if skipList[sobj.Name] || !sobj.Queryable {
			continue
		}

		describeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		describeJSON, err := s.GetSObjectResource(describeCtx, sobj.Name)
		cancel()
		if err != nil {
			s.log.Warnf("Skipping SObject %s: failed to describe: %v", sobj.Name, err)
			continue
		}

		fields, err := extractFieldNames(describeJSON)
		if err != nil {
			s.log.Warnf("Skipping SObject %s: failed to extract fields: %v", sobj.Name, err)
			continue
		}

		s.sobjectList = append(s.sobjectList, SObjectInfo{
			Name:   sobj.Name,
			Fields: fields,
		})
	}

	s.log.Infof("Loaded %d queryable SObjects for batching", len(s.sobjectList))
	return nil
}


func (s *Client) fetchNextRecordsPage(ctx context.Context, nextURL string) ([]byte, error) {
	apiUrl, err := url.Parse(s.orgURL + nextURL)
	if err != nil {
		return nil, fmt.Errorf("invalid next records URL: %w", err)
	}

	return s.callSalesforceAPI(ctx, apiUrl)
}

func extractFieldNames(describeJSON []byte) ([]string, error) {
	var dr DescribeResult
	if err := json.Unmarshal(describeJSON, &dr); err != nil {
		return nil, err
	}

	fields := make([]string, 0, len(dr.Fields))
	for _, f := range dr.Fields {
		fields = append(fields, f.Name)
	}

	return fields, nil
}

func buildSOQL(objectName string, fields []string) string {
	fieldList := strings.Join(fields, ", ")
	return fmt.Sprintf("SELECT %s FROM %s", fieldList, objectName)
}
