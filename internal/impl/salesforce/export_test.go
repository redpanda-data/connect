// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"context"
	"errors"
	"time"
)

// WaitReady blocks until every per-topic subscription owned by the input's
// gRPC client is established, or ctx is cancelled. Test-only: production
// code should not rely on this.
func (s *salesforceCDCInput) WaitReady(ctx context.Context) error {
	for {
		e := s.executor.Load()
		if e != nil {
			subs := e.grpcClient.Subscriptions()
			if len(subs) >= len(s.topicSpecs) && len(subs) > 0 {
				for _, sub := range subs {
					if err := sub.WaitReady(ctx); err != nil {
						return err
					}
				}
				return nil
			}
			select {
			case <-e.stopSig.HasStoppedChan():
				return errors.New("not connected")
			default:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
