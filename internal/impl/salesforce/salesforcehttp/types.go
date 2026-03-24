// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// types.go defines core data structures, response models, and enums for the Salesforce processor.
// It includes input query types, API response DTOs, output message formats, and resource type constants.

package salesforcehttp

import "encoding/json"

/*** Input / DTOs ***/

// SalesforceAuthResponse represents the response from the salesforce auth API
// We are using SalesforceAuthResponse in this context to get the whole auth object directly from Salesforce and parse the bearer token
type SalesforceAuthResponse struct {
	AccessToken    string `json:"access_token"`
	Signature      string `json:"signature"`
	Scope          string `json:"scope"`
	InstanceUrl    string `json:"instance_url"`
	Id             string `json:"id"`
	TokenType      string `json:"token_type"`
	IssuedAt       string `json:"issued_at"`
	ApiInstanceUrl string `json:"api_instance_url"`
}

// SObjectList is the response from all the available sObjects
type SObjectList struct {
	Encoding     string    `json:"encoding"`
	MaxBatchSize int       `json:"maxBatchSize"`
	Sobjects     []SObject `json:"sobjects"`
}

// SObject is the minimal representation of an sObject
type SObject struct {
	Name      string `json:"name"`
	Queryable bool   `json:"queryable"`
}

// DescribeResult sObject result
type DescribeResult struct {
	Fields []struct {
		Name       string `json:"name"`
		Updateable bool   `json:"updateable"`
	} `json:"fields"`
}

// ParallelSlot tracks the fetch state for one parallel worker slot.
type ParallelSlot struct {
	SObjectIndex  int    `json:"sobject_index"`
	SObjectName   string `json:"sobject_name"`
	NextURL       string `json:"next_url"`
	GraphQLCursor string `json:"graphql_cursor,omitempty"`
}

// QueryResult of the salesforce search query
type QueryResult struct {
	TotalSize      int               `json:"totalSize"`
	Done           bool              `json:"done"`
	NextRecordsUrl string            `json:"nextRecordsUrl,omitempty"`
	Records        []json.RawMessage `json:"records"`
}

// GraphQLPageInfo holds pagination info from a Salesforce GraphQL response.
type GraphQLPageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

// GraphQLEdge wraps a single node in a Salesforce GraphQL edges array.
type GraphQLEdge struct {
	Node json.RawMessage `json:"node"`
}
