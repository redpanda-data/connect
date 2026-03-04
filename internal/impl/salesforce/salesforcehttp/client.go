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
	"sync"
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

	body, err := s.callSalesforceQueryAPI(ctx, apiUrl)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// callSalesforceQueryAPI is like callSalesforceAPI but adds the Sforce-Query-Options
// batchSize header so Salesforce returns at most queryBatchSize records per page.
func (s *Client) callSalesforceQueryAPI(ctx context.Context, u *url.URL) ([]byte, error) {
	s.log.Debugf("Query API call: %s", u.String())

	if s.getBearerToken() == "" {
		if err := s.updateAndSetBearerToken(ctx); err != nil {
			return nil, err
		}
	}

	body, err := s.doSalesforceQueryRequest(ctx, u)
	if err == nil {
		return body, nil
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

	retryBody, retryErr := s.doSalesforceQueryRequest(ctx, u)
	if retryErr != nil {
		return nil, fmt.Errorf("request failed: %w", retryErr)
	}

	return retryBody, nil
}

func (s *Client) doSalesforceQueryRequest(ctx context.Context, u *url.URL) ([]byte, error) {
	newReq := func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Redpanda-Connect")
		req.Header.Set("Authorization", "Bearer "+s.getBearerToken())
		req.Header.Set("Sforce-Query-Options", fmt.Sprintf("batchSize=%d", s.queryBatchSize))
		return req, nil
	}

	return DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
}

// SObjectInfo holds basic info about an SObject
type SObjectInfo struct {
	Name   string
	Fields []string // Cached field names
}

// Cursor represents the current position in the batch processing.
// Slots drives parallel fetching; each slot independently tracks its pagination state.
type Cursor struct {
	Slots      []ParallelSlot `json:"slots,omitempty"`
	NextAssign int            `json:"next_assign"`
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

	// sobjectList is populated once and then read-only; mu protects the initial load.
	sobjectListMu sync.Mutex
	sobjectList   []SObjectInfo

	// queryBatchSize controls the Sforce-Query-Options batchSize header (200–2000).
	// Smaller values produce faster individual responses at the cost of more pages.
	queryBatchSize int

	// SObjects that returned 400 during data fetch — never retry them
	unsupportedSObjects map[string]struct{}
}

func (s *Client) getBearerToken() string {
	v, _ := s.bearerToken.Load().(string)
	return v
}

// NewClient is the constructor for a Client object
func NewClient(orgURL, clientID, clientSecret, apiVersion string, maxRetries, queryBatchSize int, httpClient *http.Client, log *service.Logger, m *service.Metrics) (*Client, error) {
	if queryBatchSize < 200 {
		queryBatchSize = 200
	} else if queryBatchSize > 2000 {
		queryBatchSize = 2000
	}
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
		queryBatchSize:      queryBatchSize,
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

// GetNextBatchParallel fetches up to parallelFetch SObjects concurrently per call.
// It maintains a slot-based cursor so each slot independently tracks its pagination state.
func (s *Client) GetNextBatchParallel(ctx context.Context, cursor Cursor, parallelFetch int) (service.MessageBatch, Cursor, bool, error) {
	// Load the SObject list exactly once. The mutex prevents concurrent reloads which
	// would produce a list with a different length than the cursor's slot indices expect.
	s.sobjectListMu.Lock()
	if len(s.sobjectList) == 0 {
		if err := s.loadSObjectList(ctx); err != nil {
			s.sobjectListMu.Unlock()
			return nil, cursor, false, err
		}
	}
	// Capture a local snapshot so goroutines use a consistent view even if the field
	// is ever replaced in a future call.
	sobjectList := s.sobjectList
	s.sobjectListMu.Unlock()

	// Fill any empty slot capacity with the next unassigned SObjects
	for len(cursor.Slots) < parallelFetch && cursor.NextAssign < len(sobjectList) {
		next := sobjectList[cursor.NextAssign]
		cursor.Slots = append(cursor.Slots, ParallelSlot{
			SObjectIndex: cursor.NextAssign,
			SObjectName:  next.Name,
		})
		cursor.NextAssign++
	}

	if len(cursor.Slots) == 0 {
		return nil, cursor, true, nil // all SObjects processed
	}

	type slotResult struct {
		slotIdx int
		records []json.RawMessage
		nextURL string
		done    bool
		skipped bool
	}

	results := make([]slotResult, len(cursor.Slots))
	var wg sync.WaitGroup

	for i, slot := range cursor.Slots {
		wg.Add(1)
		go func(i int, slot ParallelSlot) {
			defer wg.Done()

			// Skip permanently unsupported SObjects
			if _, bad := s.unsupportedSObjects[slot.SObjectName]; bad {
				results[i] = slotResult{slotIdx: i, done: true, skipped: true}
				return
			}

			var raw []byte
			var err error

			if slot.NextURL != "" {
				raw, err = s.fetchNextRecordsPage(ctx, slot.NextURL)
			} else {
				if slot.SObjectIndex >= len(sobjectList) {
					s.log.Warnf("Slot index %d out of range (list has %d items), skipping %s", slot.SObjectIndex, len(sobjectList), slot.SObjectName)
					results[i] = slotResult{slotIdx: i, done: true, skipped: true}
					return
				}
				soql := buildSOQL(slot.SObjectName, sobjectList[slot.SObjectIndex].Fields)
				raw, err = s.GetSObjectData(ctx, soql)
				if err != nil {
					var httpErr *HTTPError
					if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusBadRequest {
						s.unsupportedSObjects[slot.SObjectName] = struct{}{}
						s.log.Warnf("Skipping SObject %s permanently: query not supported (400): query=%q reason=%s",
							slot.SObjectName, soql, httpErr.Body)
						results[i] = slotResult{slotIdx: i, done: true, skipped: true}
						return
					}
					if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
						s.unsupportedSObjects[slot.SObjectName] = struct{}{}
						s.log.Warnf("Skipping SObject %s permanently: query timed out: query=%q", slot.SObjectName, soql)
						results[i] = slotResult{slotIdx: i, done: true, skipped: true}
						return
					}
					if strings.Contains(err.Error(), "request header list") {
						s.unsupportedSObjects[slot.SObjectName] = struct{}{}
						s.log.Warnf("Skipping SObject %s permanently: query URL too large (too many fields)", slot.SObjectName)
						results[i] = slotResult{slotIdx: i, done: true, skipped: true}
						return
					}
					s.log.Errorf("Failed to fetch SObject %s: %v", slot.SObjectName, err)
					results[i] = slotResult{slotIdx: i} // keep slot, retry next trigger
					return
				}
			}

			var qr QueryResult
			if err := json.Unmarshal(raw, &qr); err != nil {
				s.log.Errorf("Failed to unmarshal response for SObject %s: %v", slot.SObjectName, err)
				results[i] = slotResult{slotIdx: i} // keep slot, retry next trigger
				return
			}

			results[i] = slotResult{
				slotIdx: i,
				records: qr.Records,
				nextURL: qr.NextRecordsUrl,
				done:    qr.NextRecordsUrl == "",
			}
		}(i, slot)
	}

	wg.Wait()

	// Build output batch and updated slot list
	var batch service.MessageBatch
	newSlots := make([]ParallelSlot, 0, len(cursor.Slots))

	for i, res := range results {
		slot := cursor.Slots[res.slotIdx]

		for _, record := range res.records {
			msg := service.NewMessage(record)
			msg.MetaSet("sobject", slot.SObjectName)
			batch = append(batch, msg)
		}

		if !res.done {
			// SObject has more pages — update NextURL and keep slot
			newSlots = append(newSlots, ParallelSlot{
				SObjectIndex: slot.SObjectIndex,
				SObjectName:  slot.SObjectName,
				NextURL:      res.nextURL,
			})
		} else {
			// Slot is done — assign next available SObject if any
			if cursor.NextAssign < len(sobjectList) {
				next := sobjectList[cursor.NextAssign]
				newSlots = append(newSlots, ParallelSlot{
					SObjectIndex: cursor.NextAssign,
					SObjectName:  next.Name,
				})
				cursor.NextAssign++
			}
			if !res.skipped && len(res.records) > 0 {
				s.log.Debugf("SObject %s fully fetched (slot %d)", slot.SObjectName, i)
			}
		}
	}

	cursor.Slots = newSlots
	done := len(cursor.Slots) == 0

	return batch, cursor, done, nil
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

// loadSObjectList fetches the full SObject catalogue and describes each one in parallel
// (up to 20 concurrent describe calls) to build the ordered field list for SOQL generation.
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
		"AIUpdateRecordEvent":              true,
		"AccountUserTerritory2View":        true,
		"ApexTypeImplementor":      true,
		"ActionableListMember":     true,
		"ApexPageInfo":             true,
		"AuraDefinitionBundleInfo": true,
		"AuraDefinitionInfo":       true,
		"AppTabMember":             true,
		"Campaign":                 true,
		"ColorDefinition":          true,
		"ContentDocumentLink":      true,
		"ContentFolderItem":        true,
		"ContentFolderMember":      true,
		"DataType":                 true,
		"DatacloudAddress":         true,
		"DataStatistics":           true,
		"EntityParticle":           true,
		"FieldChangeSnapshot":      true,
		"FieldDefinition":          true,
		"EntityDefinition":         true,
		"FlexQueueItem":            true,
		"FlowDefinitionView":       true,
	}

	// Collect queryable candidates, preserving order.
	type candidate struct {
		idx  int
		name string
	}
	var candidates []candidate
	for i, sobj := range list.Sobjects {
		if skipList[sobj.Name] || !sobj.Queryable {
			continue
		}
		candidates = append(candidates, candidate{i, sobj.Name})
	}

	s.log.Infof("Describing %d queryable SObjects in parallel...", len(candidates))

	// Describe all SObjects concurrently (semaphore limits to 20 in-flight at once).
	const describeConcurrency = 20
	type describeResult struct {
		pos    int
		info   SObjectInfo
		failed bool
	}
	results := make([]describeResult, len(candidates))
	sem := make(chan struct{}, describeConcurrency)
	var wg sync.WaitGroup

	for pos, c := range candidates {
		wg.Add(1)
		sem <- struct{}{}
		go func(pos int, name string) {
			defer wg.Done()
			defer func() { <-sem }()

			describeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			describeJSON, err := s.GetSObjectResource(describeCtx, name)
			cancel()
			if err != nil {
				s.log.Warnf("Skipping SObject %s: failed to describe: %v", name, err)
				results[pos] = describeResult{pos: pos, failed: true}
				return
			}
			fields, err := extractFieldNames(describeJSON)
			if err != nil {
				s.log.Warnf("Skipping SObject %s: failed to extract fields: %v", name, err)
				results[pos] = describeResult{pos: pos, failed: true}
				return
			}
			results[pos] = describeResult{pos: pos, info: SObjectInfo{Name: name, Fields: fields}}
		}(pos, c.name)
	}
	wg.Wait()

	sobjectList := make([]SObjectInfo, 0, len(candidates))
	for _, res := range results {
		if !res.failed {
			sobjectList = append(sobjectList, res.info)
		}
	}
	s.sobjectList = sobjectList

	s.log.Infof("Loaded %d queryable SObjects for batching", len(s.sobjectList))
	return nil
}


func (s *Client) fetchNextRecordsPage(ctx context.Context, nextURL string) ([]byte, error) {
	apiUrl, err := url.Parse(s.orgURL + nextURL)
	if err != nil {
		return nil, fmt.Errorf("invalid next records URL: %w", err)
	}

	return s.callSalesforceQueryAPI(ctx, apiUrl)
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
