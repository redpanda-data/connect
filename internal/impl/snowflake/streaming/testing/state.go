/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package testing

import (
	"crypto/rand"
	"encoding/base64"
	"slices"
	"sync"
)

// ChannelState holds the state of a single channel
type ChannelState struct {
	Database             string
	Schema               string
	Table                string
	Channel              string
	ClientSequencer      int64
	RowSequencer         int64
	PersistedOffsetToken string
	EncryptionKey        string
	EncryptionKeyID      int64
	IsOpen               bool
}

// ServerState manages the in-memory state of the mock Snowflake server
type ServerState struct {
	mu sync.RWMutex

	// Channel state keyed by "database.schema.table.channel"
	channels map[string]*ChannelState

	// Prefix for client IDs
	clientPrefix string

	// Deployment ID
	deploymentID int64

	// GCS configuration
	gcsBucket     string
	gcsPathPrefix string

	// Registered blobs
	registeredBlobs []string
}

// NewServerState creates a new server state
func NewServerState() *ServerState {
	return &ServerState{
		channels:        make(map[string]*ChannelState),
		clientPrefix:    "test_client",
		deploymentID:    12345,
		registeredBlobs: make([]string, 0),
	}
}

// SetGCSConfig sets the GCS configuration
func (s *ServerState) SetGCSConfig(bucket, pathPrefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gcsBucket = bucket
	s.gcsPathPrefix = pathPrefix
}

// GetGCSConfig returns the GCS configuration
func (s *ServerState) GetGCSConfig() (bucket, pathPrefix string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gcsBucket, s.gcsPathPrefix
}

// GetClientPrefix returns the client prefix
func (s *ServerState) GetClientPrefix() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clientPrefix
}

// GetDeploymentID returns the deployment ID
func (s *ServerState) GetDeploymentID() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deploymentID
}

func channelKey(database, schema, table, channel string) string {
	return database + "." + schema + "." + table + "." + channel
}

// GetChannel returns the channel state
func (s *ServerState) GetChannel(database, schema, table, channel string) (*ChannelState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := channelKey(database, schema, table, channel)
	ch, ok := s.channels[key]
	return ch, ok
}

// OpenChannel opens a channel and returns the initial state
func (s *ServerState) OpenChannel(database, schema, table, channel string) *ChannelState {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := channelKey(database, schema, table, channel)

	// If channel already exists and is open, increment client sequencer
	if ch, exists := s.channels[key]; exists && ch.IsOpen {
		ch.ClientSequencer++
		return ch
	}

	// Create new channel with a valid base64-encoded encryption key
	// Generate a 256-bit (32 byte) random key and base64 encode it
	keyBytes := make([]byte, 32)
	_, _ = rand.Read(keyBytes) // We can ignore errors in test code
	encryptionKey := base64.StdEncoding.EncodeToString(keyBytes)

	ch := &ChannelState{
		Database:             database,
		Schema:               schema,
		Table:                table,
		Channel:              channel,
		ClientSequencer:      0,
		RowSequencer:         0,
		PersistedOffsetToken: "",
		EncryptionKey:        encryptionKey,
		EncryptionKeyID:      1,
		IsOpen:               true,
	}
	s.channels[key] = ch
	return ch
}

// DropChannel drops a channel
func (s *ServerState) DropChannel(database, schema, table, channel string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := channelKey(database, schema, table, channel)
	if ch, exists := s.channels[key]; exists {
		ch.IsOpen = false
		return true
	}
	return false
}

// UpdateChannelOffset updates the persisted offset token for a channel
func (s *ServerState) UpdateChannelOffset(database, schema, table, channel, offsetToken string, clientSequencer, rowSequencer int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := channelKey(database, schema, table, channel)
	if ch, exists := s.channels[key]; exists {
		ch.PersistedOffsetToken = offsetToken
		ch.ClientSequencer = clientSequencer
		ch.RowSequencer = rowSequencer
	}
}

// RegisterBlob records a blob registration
func (s *ServerState) RegisterBlob(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registeredBlobs = append(s.registeredBlobs, path)
}

// GetRegisteredBlobs returns all registered blobs
func (s *ServerState) GetRegisteredBlobs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.registeredBlobs)
}
