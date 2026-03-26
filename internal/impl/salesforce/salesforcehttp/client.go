// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

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
	"maps"
	"net/http"
	"net/url"
	"regexp"
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
		return nil, fmt.Errorf("refresh token: %w", err)
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
	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()

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
	s.instanceURL.Store(result.InstanceUrl)

	// Extract org ID from the identity URL: https://login.salesforce.com/id/{orgId}/{userId}
	if result.Id != "" {
		parts := strings.Split(strings.TrimRight(result.Id, "/"), "/")
		if len(parts) >= 2 {
			s.tenantID.Store(parts[len(parts)-2])
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
		return nil, fmt.Errorf("refresh token: %w", err)
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
	instanceURL  atomic.Value
	tenantID     atomic.Value
	httpClient   *http.Client
	retryOpts    RetryOptions
	log          *service.Logger

	// tokenMu serialises concurrent token refresh calls so that a single 401 response
	// shared across multiple goroutines triggers only one actual OAuth2 refresh.
	tokenMu sync.Mutex

	// sobjectList is populated once and then read-only; mu protects the initial load.
	// sobjectListLoaded is set to true after the first load attempt (success or failure).
	// sobjectListErr holds any error from the load so it can be re-returned on subsequent calls.
	sobjectListMu     sync.Mutex
	sobjectList       []SObjectInfo
	sobjectListLoaded bool
	sobjectListErr    error

	// queryBatchSize controls the Sforce-Query-Options batchSize header (200–2000).
	// Smaller values produce faster individual responses at the cost of more pages.
	queryBatchSize int

	// sobjectsMu protects unsupportedSObjects and graphqlFallbackObjects.
	sobjectsMu sync.RWMutex

	// SObjects that returned 400 during data fetch — never retry them
	unsupportedSObjects map[string]struct{}

	// SObjects whose REST query URL is too large — fetch via GraphQL instead
	graphqlFallbackObjects map[string]struct{}

	// restObjects, when non-empty, limits the REST snapshot to only these SObjects.
	restObjects map[string]struct{}
}

func (s *Client) getBearerToken() string {
	v, _ := s.bearerToken.Load().(string)
	return v
}

// ClientConfig holds the configuration for creating a new Salesforce HTTP client.
type ClientConfig struct {
	OrgURL         string
	ClientID       string
	ClientSecret   string
	APIVersion     string
	MaxRetries     int
	QueryBatchSize int
	HTTPClient     *http.Client
	Logger         *service.Logger
	Metrics        *service.Metrics
}

// NewClient is the constructor for a Client object
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.QueryBatchSize < 200 {
		cfg.QueryBatchSize = 200
	} else if cfg.QueryBatchSize > 2000 {
		cfg.QueryBatchSize = 2000
	}
	return &Client{
		log:          cfg.Logger,
		orgURL:       cfg.OrgURL,
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		apiVersion:   cfg.APIVersion,
		retryOpts: RetryOptions{
			MaxRetries: cfg.MaxRetries,
		},
		httpClient:             metrics.NewInstrumentedClient(cfg.Metrics, "salesforce_http", cfg.HTTPClient),
		queryBatchSize:         cfg.QueryBatchSize,
		unsupportedSObjects:    make(map[string]struct{}),
		graphqlFallbackObjects: make(map[string]struct{}),
		restObjects:            make(map[string]struct{}),
	}, nil
}

// SetRestObjects limits the REST snapshot to only the named SObjects.
// An empty slice removes the filter (all SObjects are fetched).
func (s *Client) SetRestObjects(names []string) {
	s.restObjects = make(map[string]struct{}, len(names))
	for _, n := range names {
		s.restObjects[n] = struct{}{}
	}
}

// BearerToken returns the current OAuth bearer token.
func (s *Client) BearerToken() string { return s.getBearerToken() }

// InstanceURL returns the Salesforce instance URL obtained during authentication.
func (s *Client) InstanceURL() string { v, _ := s.instanceURL.Load().(string); return v }

// TenantID returns the Salesforce org/tenant ID extracted from the identity URL.
func (s *Client) TenantID() string { v, _ := s.tenantID.Load().(string); return v }

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
		return nil, fmt.Errorf("marshal GraphQL payload: %w", err)
	}

	return s.callSalesforceAPIWithBody(ctx, apiUrl, bodyBytes)
}

// RestQueryPage fetches one page of a SOQL query via the REST API.
// Pass an empty nextURL on the first call; pass the returned nextURL on each subsequent call.
// Returns (batch, nextURL, error) where nextURL is empty when there are no more pages.
func (s *Client) RestQueryPage(ctx context.Context, soql, nextURL string) (service.MessageBatch, string, error) {
	var (
		raw []byte
		err error
	)
	if nextURL == "" {
		raw, err = s.GetSObjectData(ctx, soql)
	} else {
		raw, err = s.fetchNextRecordsPage(ctx, nextURL)
	}
	if err != nil {
		return nil, "", err
	}

	var qr QueryResult
	if err := json.Unmarshal(raw, &qr); err != nil {
		return nil, "", fmt.Errorf("parse query result: %w", err)
	}

	var batch service.MessageBatch
	for _, record := range qr.Records {
		msg := service.NewMessage(record)
		msg.MetaSet("soql", soql)
		msg.MetaSet("total_size", strconv.Itoa(qr.TotalSize))
		batch = append(batch, msg)
	}

	return batch, qr.NextRecordsUrl, nil
}

// GraphQLQueryPage fetches one page of a GraphQL query.
// Pass an empty cursor on the first call; pass the returned cursor on each subsequent call.
// Returns (batch, nextCursor, error) where nextCursor is empty when there are no more pages.
// The query must include pageInfo { hasNextPage endCursor } in the selection set.
func (s *Client) GraphQLQueryPage(ctx context.Context, query, cursor string) (service.MessageBatch, string, error) {
	q := injectGraphQLCursor(query, cursor)
	if cursor != "" && q == query {
		s.log.Warn("GraphQL cursor injection failed: query unchanged, cannot paginate further")
		return nil, "", nil
	}

	raw, err := s.GraphQL(ctx, q)
	if err != nil {
		return nil, "", err
	}

	var result map[string]json.RawMessage
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, "", fmt.Errorf("parse GraphQL response: %w", err)
	}

	edges, pageInfo, found := findGraphQLEdges(result)
	if !found {
		// No edges structure found — return raw response as single message
		msg := service.NewMessage(raw)
		msg.MetaSet("query", query)
		return service.MessageBatch{msg}, "", nil
	}

	var batch service.MessageBatch
	for _, edge := range edges {
		var e GraphQLEdge
		if err := json.Unmarshal(edge, &e); err != nil {
			s.log.Warnf("unmarshal GraphQL edge: %v", err)
			continue
		}
		msg := service.NewMessage(e.Node)
		msg.MetaSet("query", query)
		batch = append(batch, msg)
	}

	nextCursor := ""
	if pageInfo.HasNextPage {
		nextCursor = pageInfo.EndCursor
	}

	return batch, nextCursor, nil
}

var (
	// reGraphQLFirstParam matches existing (first: N) argument.
	reGraphQLFirstParam = regexp.MustCompile(`\(first:\s*\d+`)
	// reGraphQLPascalObject matches the first PascalCase object name followed by {
	// (i.e. Salesforce SObject names like FlowOrchestration, Account, etc.)
	reGraphQLPascalObject = regexp.MustCompile(`\b([A-Z][a-zA-Z0-9_]*)\s*\{`)
)

// injectGraphQLCursor adds after: "cursor" into the query for the next page.
// Handles two cases:
//   - Query has (first: N): injects , after: "cursor" inside existing args
//   - Query has no args: injects (after: "cursor") before { of the first PascalCase object
func injectGraphQLCursor(query, cursor string) string {
	if cursor == "" {
		return query
	}
	afterClause := fmt.Sprintf(`after: "%s"`, cursor)

	// Case 1: existing (first: N) arg — append after inside parens
	if reGraphQLFirstParam.MatchString(query) {
		return reGraphQLFirstParam.ReplaceAllString(query, "$0, "+afterClause)
	}

	// Case 2: no args — inject (after: "cursor") before { of first PascalCase object
	replaced := false
	return reGraphQLPascalObject.ReplaceAllStringFunc(query, func(match string) string {
		if replaced {
			return match
		}
		parts := reGraphQLPascalObject.FindStringSubmatch(match)
		replaced = true
		return fmt.Sprintf("%s(%s) {", parts[1], afterClause)
	})
}

// findGraphQLEdges recursively traverses a GraphQL response to find the first node
// containing an "edges" array, returning the edges and pageInfo.
func findGraphQLEdges(data map[string]json.RawMessage) ([]json.RawMessage, GraphQLPageInfo, bool) {
	if edgesRaw, ok := data["edges"]; ok {
		var edges []json.RawMessage
		if err := json.Unmarshal(edgesRaw, &edges); err == nil {
			var pageInfo GraphQLPageInfo
			if piRaw, ok := data["pageInfo"]; ok {
				_ = json.Unmarshal(piRaw, &pageInfo)
			}
			return edges, pageInfo, true
		}
	}
	for _, v := range data {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(v, &nested); err == nil {
			if edges, pageInfo, ok := findGraphQLEdges(nested); ok {
				return edges, pageInfo, ok
			}
		}
	}
	return nil, GraphQLPageInfo{}, false
}

// GetNextBatchParallel fetches up to parallelFetch SObjects concurrently per call.
// It maintains a slot-based cursor so each slot independently tracks its pagination state.
func (s *Client) GetNextBatchParallel(ctx context.Context, cursor Cursor, parallelFetch int) (service.MessageBatch, Cursor, bool, error) {
	// Load the SObject list exactly once. The mutex prevents concurrent reloads which
	// would produce a list with a different length than the cursor's slot indices expect.
	s.sobjectListMu.Lock()
	if !s.sobjectListLoaded {
		err := s.loadSObjectList(ctx)
		s.sobjectListLoaded = true
		s.sobjectListErr = err
		if err != nil {
			s.log.Errorf("load SObject list: %v", err)
		}
	}
	if s.sobjectListErr != nil {
		s.sobjectListMu.Unlock()
		return nil, cursor, false, s.sobjectListErr
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
		// cursor.NextAssign is past the end of the current sobjectList.
		// This is expected when all SObjects have been processed, but can also happen
		// if the sobjectList shrank (e.g. more items added to the static skipList) and a
		// stale checkpoint had NextAssign beyond the new list length.
		// Detect the latter: if NextAssign is past the end but the list is non-empty,
		// reset the cursor so the snapshot restarts cleanly instead of silently completing.
		if cursor.NextAssign > len(sobjectList) && len(sobjectList) > 0 {
			s.log.Warnf("Checkpoint NextAssign=%d exceeds current SObject list length=%d — SObject list likely changed. Resetting cursor to restart snapshot.", cursor.NextAssign, len(sobjectList))
			cursor.NextAssign = 0
			cursor.Slots = nil
			// Re-fill slots with the reset cursor
			for len(cursor.Slots) < parallelFetch && cursor.NextAssign < len(sobjectList) {
				next := sobjectList[cursor.NextAssign]
				cursor.Slots = append(cursor.Slots, ParallelSlot{
					SObjectIndex: cursor.NextAssign,
					SObjectName:  next.Name,
				})
				cursor.NextAssign++
			}
		}
		if len(cursor.Slots) == 0 {
			return nil, cursor, true, nil // all SObjects processed
		}
	}

	// Snapshot map state before launching goroutines. Goroutines must not write to
	// s.unsupportedSObjects or s.graphqlFallbackObjects directly — concurrent map
	// reads+writes cause a fatal panic in Go. Instead they set flags in slotResult
	// and the main goroutine applies updates after wg.Wait().
	s.sobjectsMu.RLock()
	unsupportedSnap := make(map[string]struct{}, len(s.unsupportedSObjects))
	maps.Copy(unsupportedSnap, s.unsupportedSObjects)
	graphqlFallbackSnap := make(map[string]struct{}, len(s.graphqlFallbackObjects))
	maps.Copy(graphqlFallbackSnap, s.graphqlFallbackObjects)
	s.sobjectsMu.RUnlock()

	type slotResult struct {
		slotIdx         int
		records         []json.RawMessage
		nextURL         string
		graphqlCursor   string
		done            bool
		skipped         bool
		markUnsupported bool // apply to s.unsupportedSObjects after wg.Wait
		markGraphQL     bool // apply to s.graphqlFallbackObjects after wg.Wait
	}

	results := make([]slotResult, len(cursor.Slots))
	var wg sync.WaitGroup

	for i, slot := range cursor.Slots {
		wg.Add(1)
		go func(i int, slot ParallelSlot) {
			defer wg.Done()

			// Skip permanently unsupported SObjects (read from snapshot — safe)
			if _, bad := unsupportedSnap[slot.SObjectName]; bad {
				results[i] = slotResult{slotIdx: i, done: true, skipped: true}
				return
			}

			// Use GraphQL for SObjects whose REST URL is too large (read from snapshot — safe)
			if _, useGraphQL := graphqlFallbackSnap[slot.SObjectName]; useGraphQL {
				if slot.SObjectIndex >= len(sobjectList) {
					results[i] = slotResult{slotIdx: i, done: true, skipped: true}
					return
				}
				gqlQuery := buildSObjectGraphQLQuery(slot.SObjectName, sobjectList[slot.SObjectIndex].Fields, slot.GraphQLCursor, s.queryBatchSize)
				raw, err := s.GraphQL(ctx, gqlQuery)
				if err != nil {
					s.log.Warnf("Skipping SObject %s permanently: GraphQL fallback failed: %v", slot.SObjectName, err)
					results[i] = slotResult{slotIdx: i, done: true, skipped: true, markUnsupported: true}
					return
				}
				var result map[string]json.RawMessage
				if err := json.Unmarshal(raw, &result); err != nil {
					s.log.Warnf("Skipping SObject %s permanently: parse GraphQL response: %v", slot.SObjectName, err)
					results[i] = slotResult{slotIdx: i, done: true, skipped: true, markUnsupported: true}
					return
				}
				edges, pageInfo, found := findGraphQLEdges(result)
				if !found {
					s.log.Warnf("Skipping SObject %s permanently: no edges in GraphQL response", slot.SObjectName)
					results[i] = slotResult{slotIdx: i, done: true, skipped: true, markUnsupported: true}
					return
				}
				var records []json.RawMessage
				for _, edge := range edges {
					var e GraphQLEdge
					if err := json.Unmarshal(edge, &e); err != nil {
						continue
					}
					records = append(records, e.Node)
				}
				var nextCursor string
				if pageInfo.HasNextPage {
					nextCursor = pageInfo.EndCursor
				}
				results[i] = slotResult{
					slotIdx:       i,
					records:       records,
					graphqlCursor: nextCursor,
					done:          !pageInfo.HasNextPage,
				}
				return
			}

			var raw []byte
			var err error

			if slot.NextURL != "" {
				raw, err = s.fetchNextRecordsPage(ctx, slot.NextURL)
				if err != nil {
					s.log.Errorf("fetch next page for SObject %s: %v", slot.SObjectName, err)
					results[i] = slotResult{slotIdx: i} // keep slot, retry next trigger
					return
				}
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
					if errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusBadRequest || httpErr.StatusCode == http.StatusForbidden || httpErr.StatusCode == http.StatusInternalServerError) {
						s.log.Warnf("Skipping SObject %s permanently: query not supported (%d): query=%q reason=%s",
							slot.SObjectName, httpErr.StatusCode, soql, httpErr.Body)
						results[i] = slotResult{slotIdx: i, done: true, skipped: true, markUnsupported: true}
						return
					}
					if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
						s.log.Warnf("Skipping SObject %s permanently: query timed out: query=%q", slot.SObjectName, soql)
						results[i] = slotResult{slotIdx: i, done: true, skipped: true, markUnsupported: true}
						return
					}
					if strings.Contains(err.Error(), "request header list") {
						s.log.Warnf("SObject %s REST URL too large, will retry via GraphQL", slot.SObjectName)
						results[i] = slotResult{slotIdx: i, markGraphQL: true} // keep slot, retry via GraphQL next trigger
						return
					}
					s.log.Errorf("fetch SObject %s: %v", slot.SObjectName, err)
					results[i] = slotResult{slotIdx: i} // keep slot, retry next trigger
					return
				}
			}

			var qr QueryResult
			if err := json.Unmarshal(raw, &qr); err != nil {
				s.log.Errorf("unmarshal response for SObject %s: %v", slot.SObjectName, err)
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

	// Apply map updates from goroutines (safe — no goroutines running at this point)
	s.sobjectsMu.Lock()
	for i, res := range results {
		if res.markUnsupported {
			s.unsupportedSObjects[cursor.Slots[i].SObjectName] = struct{}{}
		}
		if res.markGraphQL {
			s.graphqlFallbackObjects[cursor.Slots[i].SObjectName] = struct{}{}
		}
	}
	s.sobjectsMu.Unlock()

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
			// SObject has more pages — update pagination state and keep slot
			newSlots = append(newSlots, ParallelSlot{
				SObjectIndex:  slot.SObjectIndex,
				SObjectName:   slot.SObjectName,
				NextURL:       res.nextURL,
				GraphQLCursor: res.graphqlCursor,
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
		return nil, fmt.Errorf("refresh token: %w", err)
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
	if len(s.restObjects) > 0 {
		wanted := make([]string, 0, len(s.restObjects))
		for n := range s.restObjects {
			wanted = append(wanted, fmt.Sprintf("%q", n))
		}
		s.log.Infof("rest_objects filter active, looking for: %v", wanted)
	}

	raw, err := s.GetAllSObjectResources(ctx)
	if err != nil {
		return err
	}
	s.log.Infof("SObject list response size: %.1f KB", float64(len(raw))/1024)

	var list SObjectList
	if err := json.Unmarshal(raw, &list); err != nil {
		return fmt.Errorf("parse SObject list (response: %.200s): %w", raw, err)
	}
	s.log.Infof("API returned %d total SObjects", len(list.Sobjects))
	if len(list.Sobjects) == 0 {
		return fmt.Errorf("salesforce returned 0 SObjects — check credentials and org URL (response: %.200s)", raw)
	}

	skipList := map[string]bool{
		"AIUpdateRecordEvent":         true,
		"AccountUserTerritory2View":   true,
		"ActionableListMember":        true,
		"ApexPageInfo":                true,
		"ApexTypeImplementor":         true,
		"AppTabMember":                true,
		"AuraDefinitionBundleInfo":    true,
		"AuraDefinitionInfo":          true,
		"CategoryNode":                true,
		"ColorDefinition":             true,
		"ConnectedApplication":        true,
		"ContentDocumentLink":         true,
		"ContentFolderItem":           true,
		"ContentFolderMember":         true,
		"DataStatistics":              true,
		"DataType":                    true,
		"DatacloudAddress":            true,
		"DecisionTable":               true,
		"DecisionTableDatasetLink":    true,
		"EntityDefinition":            true,
		"EntityParticle":              true,
		"ExpressionSetView":           true,
		"FieldChangeSnapshot":         true,
		"FieldDefinition":             true,
		"FlexQueueItem":               true,
		"FlowDefinitionView":          true,
		"FlowTestView":                true,
		"FlowVariableView":            true,
		"FlowVersionView":             true,
		"IconDefinition":              true,
		"IdeaComment":                 true,
		"ListViewChartInstance":       true,
		"LoyaltyPgmMbrPromEligView":   true,
		"NetworkUserHistoryRecent":    true,
		"ObjectUserTerritory2View":    true,
		"OutgoingEmail":               true,
		"OutgoingEmailRelation":       true,
		"OwnerChangeOptionInfo":       true,
		"PicklistValueInfo":           true,
		"PlatformAction":              true,
		"RecentFieldChange":           true,
		"RelatedListColumnDefinition": true,
		"RelatedListDefinition":       true,
		"RelationshipDomain":          true,
		"RelationshipInfo":            true,
		"SearchLayout":                true,
		"SiteDetail":                  true,
		"UserEntityAccess":            true,
		"UserFieldAccess":             true,
		"UserRecordAccess":            true,
		"UserSharedFeature":           true,
		"Vote":                        true,
	}

	// Collect queryable candidates, preserving order.
	type candidate struct {
		idx  int
		name string
	}
	var candidates []candidate
	s.log.Infof("SObject list contains %d entries", len(list.Sobjects))
	for i, sobj := range list.Sobjects {
		if skipList[sobj.Name] || !sobj.Queryable {
			continue
		}
		if len(s.restObjects) > 0 {
			if _, ok := s.restObjects[sobj.Name]; !ok {
				continue
			}
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
				s.log.Warnf("Skipping SObject %s: describe: %v", name, err)
				results[pos] = describeResult{pos: pos, failed: true}
				return
			}
			fields, err := extractFieldNames(describeJSON)
			if err != nil {
				s.log.Warnf("Skipping SObject %s: extract fields: %v", name, err)
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

	if len(s.sobjectList) == 0 {
		if len(s.restObjects) > 0 {
			// Report the status of each requested object specifically.
			apiObjects := make(map[string]bool, len(list.Sobjects)) // name → queryable
			for _, sobj := range list.Sobjects {
				apiObjects[sobj.Name] = sobj.Queryable
			}
			details := make([]string, 0, len(s.restObjects))
			for name := range s.restObjects {
				if queryable, found := apiObjects[name]; found {
					details = append(details, fmt.Sprintf("%q found but queryable=%v", name, queryable))
				} else {
					details = append(details, fmt.Sprintf("%q not found in API response", name))
				}
			}
			return fmt.Errorf("none of the requested rest_objects produced queryable SObjects: %v", details)
		}
		s.log.Warn("No queryable SObjects found in this org")
	}

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

// DescribeWritableFields returns the set of field names that are updateable for the given SObject.
func (s *Client) DescribeWritableFields(ctx context.Context, sobject string) (map[string]struct{}, error) {
	raw, err := s.GetSObjectResource(ctx, sobject)
	if err != nil {
		return nil, fmt.Errorf("describe %s: %w", sobject, err)
	}
	var dr DescribeResult
	if err := json.Unmarshal(raw, &dr); err != nil {
		return nil, fmt.Errorf("describe %s: parse: %w", sobject, err)
	}
	fields := make(map[string]struct{}, len(dr.Fields))
	for _, f := range dr.Fields {
		if f.Updateable {
			fields[f.Name] = struct{}{}
		}
	}
	return fields, nil
}

func buildSOQL(objectName string, fields []string) string {
	fieldList := strings.Join(fields, ", ")
	return fmt.Sprintf("SELECT %s FROM %s", fieldList, objectName)
}

// APIVersion returns the configured Salesforce REST API version string (e.g., "v65.0").
func (s *Client) APIVersion() string { return s.apiVersion }
func (s *Client) OrgURL() string     { return s.orgURL }

// PostJSON sends an authenticated POST request with a JSON body to the given relative path.
func (s *Client) PostJSON(ctx context.Context, path string, body []byte) ([]byte, error) {
	u, err := url.Parse(s.orgURL + path)
	if err != nil {
		return nil, fmt.Errorf("invalid URL path %q: %w", path, err)
	}
	return s.callSalesforceAPIWithBody(ctx, u, body)
}

// PatchJSON sends an authenticated PATCH request with a JSON body to the given relative path.
func (s *Client) PatchJSON(ctx context.Context, path string, body []byte) ([]byte, error) {
	u, err := url.Parse(s.orgURL + path)
	if err != nil {
		return nil, fmt.Errorf("invalid URL path %q: %w", path, err)
	}
	return s.callSalesforceAPIPatch(ctx, u, body)
}

// PutCSV sends an authenticated PUT request with CSV body to the given relative path.
func (s *Client) PutCSV(ctx context.Context, path string, csvData []byte) ([]byte, error) {
	u, err := url.Parse(s.orgURL + path)
	if err != nil {
		return nil, fmt.Errorf("invalid URL path %q: %w", path, err)
	}
	return s.callSalesforceAPIPutCSV(ctx, u, csvData)
}

// GetJSON sends an authenticated GET request to the given relative path and returns the body.
func (s *Client) GetJSON(ctx context.Context, path string) ([]byte, error) {
	u, err := url.Parse(s.orgURL + path)
	if err != nil {
		return nil, fmt.Errorf("invalid URL path %q: %w", path, err)
	}
	return s.callSalesforceAPI(ctx, u)
}

// DeleteJSON sends an authenticated DELETE request to the given URL (including query params) and returns the body.
func (s *Client) DeleteJSON(ctx context.Context, rawURL string) ([]byte, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", rawURL, err)
	}
	if s.getBearerToken() == "" {
		if err := s.updateAndSetBearerToken(ctx); err != nil {
			return nil, err
		}
	}
	newReq := func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Redpanda-Connect")
		req.Header.Set("Authorization", "Bearer "+s.getBearerToken())
		return req, nil
	}
	resp, err := DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
	if err == nil {
		return resp, nil
	}
	httpErr, ok := err.(*HTTPError)
	if !ok || httpErr.StatusCode != http.StatusUnauthorized {
		return nil, err
	}
	if err := s.updateAndSetBearerToken(ctx); err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}
	return DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
}

func (s *Client) callSalesforceAPIPatch(ctx context.Context, u *url.URL, body []byte) ([]byte, error) {
	if s.getBearerToken() == "" {
		if err := s.updateAndSetBearerToken(ctx); err != nil {
			return nil, err
		}
	}
	newReq := func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, u.String(), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Redpanda-Connect")
		req.Header.Set("Authorization", "Bearer "+s.getBearerToken())
		return req, nil
	}
	resp, err := DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
	if err == nil {
		return resp, nil
	}
	httpErr, ok := err.(*HTTPError)
	if !ok || httpErr.StatusCode != http.StatusUnauthorized {
		return nil, err
	}
	if err := s.updateAndSetBearerToken(ctx); err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}
	return DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
}

func (s *Client) callSalesforceAPIPutCSV(ctx context.Context, u *url.URL, csvData []byte) ([]byte, error) {
	if s.getBearerToken() == "" {
		if err := s.updateAndSetBearerToken(ctx); err != nil {
			return nil, err
		}
	}
	newReq := func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), bytes.NewReader(csvData))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "text/csv")
		req.Header.Set("User-Agent", "Redpanda-Connect")
		req.Header.Set("Authorization", "Bearer "+s.getBearerToken())
		return req, nil
	}
	resp, err := DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
	if err == nil {
		return resp, nil
	}
	httpErr, ok := err.(*HTTPError)
	if !ok || httpErr.StatusCode != http.StatusUnauthorized {
		return nil, err
	}
	if err := s.updateAndSetBearerToken(ctx); err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}
	return DoRequestWithRetries(ctx, s.httpClient, newReq, s.retryOpts)
}

// buildSObjectGraphQLQuery constructs a Salesforce GraphQL query for a given SObject.
// All fields are wrapped in { value } as required by the Salesforce UI API GraphQL format.
// When cursor is non-empty, after: "cursor" pagination is injected.
func buildSObjectGraphQLQuery(objectName string, fields []string, cursor string, batchSize int) string {
	fieldLines := make([]string, 0, len(fields))
	for _, f := range fields {
		fieldLines = append(fieldLines, f+" { value }")
	}
	fieldSelection := strings.Join(fieldLines, "\n            ")

	args := fmt.Sprintf("first: %d", batchSize)
	if cursor != "" {
		args += fmt.Sprintf(`, after: "%s"`, cursor)
	}

	return fmt.Sprintf(`{ uiapi { query { %s(%s) { edges { node { %s } } pageInfo { hasNextPage endCursor } } } } }`,
		objectName, args, fieldSelection)
}
