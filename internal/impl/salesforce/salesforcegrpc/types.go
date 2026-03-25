// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforcegrpc

import "time"

// EventType distinguishes between CDC and Platform Event payloads.
type EventType int

const (
	// EventTypeCDC - Change Data Capture event
	EventTypeCDC EventType = iota
	// EventTypePlatform -  Platform Event
	EventTypePlatform
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
	Fields map[string]any

	// RawPayload is the full decoded Avro payload as a map.
	RawPayload map[string]any
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
	// StreamStateDisconnected - disconnected state
	StreamStateDisconnected StreamState = iota
	// StreamStateConnecting - connecting state
	StreamStateConnecting
	// StreamStateConnected - connected state
	StreamStateConnected
	// StreamStateReconnecting - reconnecting state
	StreamStateReconnecting
	// StreamStateClosing - closing state
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
	State             StreamState
	TopicName         string
	LastEventTime     time.Time
	EventsReceived    int64
	EventsDropped     int64
	ReconnectCount    int64
	BufferUtilization float64
	LastError         error
	LastErrorTime     time.Time
}
