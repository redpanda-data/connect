// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Outcomes a plan can be marked done with. The first four come from the
// bench workflow, the last two from the watchdog.
const (
	OutcomeSuccess       = "success"
	OutcomeFailure       = "failure"
	OutcomeCancelled     = "cancelled"
	OutcomeStackBusy     = "stack_busy"
	OutcomeNeverReported = "never_reported"
	OutcomeNeverStarted  = "never_started"
	// OutcomeInvalidConfig: the rendered Connect config failed lint, so the
	// run stopped before taking the lock or spending anything.
	OutcomeInvalidConfig = "invalid_config"
)

var outcomes = []string{
	OutcomeSuccess, OutcomeFailure, OutcomeCancelled, OutcomeStackBusy,
	OutcomeNeverReported, OutcomeNeverStarted, OutcomeInvalidConfig,
}

// ValidOutcome reports whether o is one of the known outcomes.
func ValidOutcome(o string) bool { return slices.Contains(outcomes, o) }

// StalePlan is a claimed plan the watchdog must report on.
type StalePlan struct {
	ID        string    `json:"plan_id"`
	Outcome   string    `json:"outcome"`
	RunID     int64     `json:"run_id,omitempty"`
	ClaimedAt time.Time `json:"claimed_at"`
}

const dispatchedSuffix = ".dispatched"

func (s *objectStore) Stale(ctx context.Context, now time.Time, notReported, notStarted time.Duration) ([]StalePlan, error) {
	if notReported <= 0 || notStarted <= 0 {
		return nil, errors.New("stale thresholds must be positive")
	}
	keys, err := s.objs.List(ctx, "plans/")
	if err != nil {
		return nil, err
	}
	out := []StalePlan{}
	for _, k := range keys {
		id, ok := strings.CutSuffix(strings.TrimPrefix(k, "plans/"), dispatchedSuffix)
		if !ok || !ValidID(id) || k != dispatchedKey(id) {
			continue
		}
		if _, done, err := readMarker[doneMarker](ctx, s.objs, doneKey(id)); err != nil {
			return nil, err
		} else if done {
			continue
		}
		c, ok, err := readMarker[claimMarker](ctx, s.objs, dispatchedKey(id))
		if err != nil {
			return nil, err
		}
		if !ok {
			continue // removed between list and read
		}
		r, started, err := readMarker[runMarker](ctx, s.objs, runKey(id))
		if err != nil {
			return nil, err
		}
		age := now.Sub(c.At)
		switch {
		case !started && age > notStarted:
			out = append(out, StalePlan{ID: id, Outcome: OutcomeNeverStarted, ClaimedAt: c.At})
		case started && age > notReported:
			out = append(out, StalePlan{ID: id, Outcome: OutcomeNeverReported, RunID: r.RunID, ClaimedAt: c.At})
		}
	}
	slices.SortFunc(out, func(a, b StalePlan) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// RequireClaimed returns an error unless p is claimed and not yet done.
func RequireClaimed(p Plan) error {
	switch {
	case !p.Claimed:
		return fmt.Errorf("plan %s is not claimed: no approval recorded", p.ID)
	case p.Done:
		return fmt.Errorf("plan %s is already done (outcome %s)", p.ID, p.Outcome)
	}
	return nil
}
