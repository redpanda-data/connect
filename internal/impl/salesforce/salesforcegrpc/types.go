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

package salesforcegrpc

import "time"

// EventType distinguishes between CDC and Platform Event payloads.
type EventType int

const (
	EventTypeCDC      EventType = iota // Change Data Capture event
	EventTypePlatform                  // Platform Event
)

// String returns a human-readable label for the EventType.
func (t EventType) String() string {
	switch t {
	case EventTypeCDC:
		return "CDC"
	case EventTypePlatform:
		return "PlatformEvent"
	default:
		return "Unknown"
	}
}

// PubSubEvent represents a decoded event from the Salesforce Pub/Sub API.
// It covers both CDC change events and Platform Events.
type PubSubEvent struct {
	// ReplayID is the opaque replay identifier for this event.
	ReplayID []byte

	// SchemaID is the Avro schema fingerprint used to encode the event.
	SchemaID string

	// TopicName is the Pub/Sub API topic this event was received on.
	TopicName string

	// Type indicates whether this is a CDC or Platform Event.
	Type EventType

	// ChangeType indicates the type of change (CREATE, UPDATE, DELETE, UNDELETE, GAP_CREATE, etc.).
	// Only populated for CDC events.
	ChangeType string

	// EntityName is the SObject type (e.g. "Account", "Contact").
	// Only populated for CDC events.
	EntityName string

	// RecordIDs contains the IDs of the records affected by the change.
	// Only populated for CDC events.
	RecordIDs []string

	// Fields contains the changed field values decoded from the Avro payload.
	Fields map[string]interface{}

	// RawPayload is the full decoded Avro payload as a map.
	RawPayload map[string]interface{}
}

// CDCEvent is a backward-compatible alias for PubSubEvent.
type CDCEvent = PubSubEvent

// SubscriptionConfig holds the parameters for a Pub/Sub API subscription.
type SubscriptionConfig struct {
	TopicName    string
	BatchSize    int32
	BufferSize   int
	ReplayPreset ReplayPreset
	ReplayID     []byte
}

// StreamState represents the lifecycle state of a subscription stream.
type StreamState int

const (
	StreamStateDisconnected StreamState = iota
	StreamStateConnecting
	StreamStateConnected
	StreamStateReconnecting
	StreamStateClosing
)

// String returns a human-readable label for the StreamState.
func (s StreamState) String() string {
	switch s {
	case StreamStateDisconnected:
		return "Disconnected"
	case StreamStateConnecting:
		return "Connecting"
	case StreamStateConnected:
		return "Connected"
	case StreamStateReconnecting:
		return "Reconnecting"
	case StreamStateClosing:
		return "Closing"
	default:
		return "Unknown"
	}
}

// SubscriptionHealth provides a point-in-time snapshot of the subscription's health.
type SubscriptionHealth struct {
	State            StreamState
	TopicName        string
	LastEventTime    time.Time
	EventsReceived   int64
	EventsDropped    int64
	ReconnectCount   int64
	BufferUtilization float64
	LastError        error
	LastErrorTime    time.Time
}
