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

// Package kubernetes provides Bento inputs for Kubernetes observability.
//
// This package implements native Kubernetes integrations that feel as natural
// as kubectl commands. System administrators can build pipelines for:
//
//   - Cluster event monitoring (kubernetes_events)
//
// All components share a common authentication layer supporting:
//   - Automatic in-cluster detection
//   - Kubeconfig file authentication
//   - Service account tokens
//   - Explicit credentials
package kubernetes

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// Backoff parameters for watch reconnection
	minBackoff = 1 * time.Second
	maxBackoff = 60 * time.Second
)

// keyCache optimizes metadata key generation by interning strings
// like "kubernetes_labels_app" to avoid repeated allocations.
var keyCache sync.Map

func getMetaKey(prefix, key string) string {
	fullKey := prefix + key
	if val, ok := keyCache.Load(fullKey); ok {
		return val.(string)
	}
	keyCache.Store(fullKey, fullKey)
	return fullKey
}

// calculateBackoff returns an exponential backoff duration with jitter.
// The backoff doubles with each attempt, capped at maxBackoff.
// Jitter adds randomness to prevent synchronized retries across instances.
func calculateBackoff(attempt int) time.Duration {
	backoff := minBackoff * time.Duration(1<<uint(attempt))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	// Add up to 25% jitter
	jitter := time.Duration(rand.Int63n(int64(backoff / 4)))
	return backoff + jitter
}
