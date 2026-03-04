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
		Name string `json:"name"`
	} `json:"fields"`
}

// ParallelSlot tracks the fetch state for one parallel worker slot.
type ParallelSlot struct {
	SObjectIndex int    `json:"sobject_index"`
	SObjectName  string `json:"sobject_name"`
	NextURL      string `json:"next_url"`
}

// QueryResult of the salesforce search query
type QueryResult struct {
	TotalSize      int               `json:"totalSize"`
	Done           bool              `json:"done"`
	NextRecordsUrl string            `json:"nextRecordsUrl,omitempty"`
	Records        []json.RawMessage `json:"records"`
}
