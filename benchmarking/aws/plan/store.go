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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var (
	// ErrConflict means a conditional write lost: the object already exists
	// (create) or changed since it was read (replace/delete).
	ErrConflict = errors.New("conflict: object already exists or changed")
	// ErrNotFound means the object does not exist.
	ErrNotFound = errors.New("not found")
)

var idPattern = regexp.MustCompile(`^[0-9a-f]{12}$`)

// ValidID reports whether id is a well-formed plan ID. Every key built from
// an ID is only built after this check passes.
func ValidID(id string) bool { return idPattern.MatchString(id) }

func checkID(id string) error {
	if !ValidID(id) {
		return fmt.Errorf("invalid plan id %q: want 12 lower-case hex characters", id)
	}
	return nil
}

// Store keeps Plans. A stored plan is immutable; dispatch and completion are
// recorded in separate marker objects so each can be written exactly once.
type Store interface {
	// Put stores a new plan and refuses to overwrite an existing one
	// (ErrConflict).
	Put(ctx context.Context, p Plan) error
	// Get returns the plan, with DispatchedRunID filled from the dispatch
	// marker when one exists. ErrNotFound when there is no such plan.
	Get(ctx context.Context, id string) (Plan, error)
	// MarkDispatched records the run that was started for the plan. The bool
	// is true only for the first caller; later callers get the plan carrying
	// the first caller's run id.
	MarkDispatched(ctx context.Context, id string, runID int64, approver string) (Plan, bool, error)
	// MarkDone records the outcome once; a second call is a no-op.
	MarkDone(ctx context.Context, id, outcome string) error
}

// Objects is the conditional-write object API the plan store and the lock
// are built on. S3 backs production; a directory backs local use and tests.
type Objects interface {
	// Create writes a new object, or returns ErrConflict if it exists.
	Create(ctx context.Context, key string, body []byte) error
	// Get returns the body and an opaque version tag, or ErrNotFound.
	Get(ctx context.Context, key string) (body []byte, etag string, err error)
	// Replace overwrites the object only if its version is still etag
	// (ErrConflict otherwise, including when it has been deleted).
	Replace(ctx context.Context, key string, body []byte, etag string) error
	// Delete removes the object only if its version is still etag
	// (ErrConflict otherwise). A missing object is not an error.
	Delete(ctx context.Context, key, etag string) error
}

func planKey(id string) string       { return "plans/" + id + ".json" }
func dispatchedKey(id string) string { return "plans/" + id + ".dispatched" }
func doneKey(id string) string       { return "plans/" + id + ".done" }

type dispatchMarker struct {
	RunID    int64     `json:"run_id"`
	Approver string    `json:"approver"`
	At       time.Time `json:"at"`
}

type doneMarker struct {
	Outcome string    `json:"outcome"`
	At      time.Time `json:"at"`
}

type objectStore struct {
	objs Objects
	now  func() time.Time
}

// NewStore returns a Store over any Objects.
func NewStore(objs Objects) Store { return &objectStore{objs: objs, now: time.Now} }

// NewS3Store returns a Store keeping plans in bucket.
func NewS3Store(api S3API, bucket string) Store { return NewStore(NewS3Objects(api, bucket)) }

// NewDirStore returns a Store keeping plans under root.
func NewDirStore(root string) Store { return NewStore(NewDirObjects(root)) }

func (s *objectStore) Put(ctx context.Context, p Plan) error {
	if err := checkID(p.ID); err != nil {
		return err
	}
	p.DispatchedRunID = 0 // the marker is the only source of truth
	body, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding plan %s: %w", p.ID, err)
	}
	if err := s.objs.Create(ctx, planKey(p.ID), body); err != nil {
		if errors.Is(err, ErrConflict) {
			return fmt.Errorf("plan %s is already stored and plans are immutable: %w", p.ID, err)
		}
		return fmt.Errorf("storing plan %s: %w", p.ID, err)
	}
	return nil
}

func (s *objectStore) Get(ctx context.Context, id string) (Plan, error) {
	if err := checkID(id); err != nil {
		return Plan{}, err
	}
	body, _, err := s.objs.Get(ctx, planKey(id))
	if err != nil {
		return Plan{}, fmt.Errorf("reading plan %s: %w", id, err)
	}
	var p Plan
	if err := json.Unmarshal(body, &p); err != nil {
		return Plan{}, fmt.Errorf("parsing plan %s: %w", id, err)
	}
	m, ok, err := s.readDispatched(ctx, id)
	if err != nil {
		return Plan{}, err
	}
	if ok {
		p.DispatchedRunID = m.RunID
	}
	return p, nil
}

func (s *objectStore) readDispatched(ctx context.Context, id string) (dispatchMarker, bool, error) {
	body, _, err := s.objs.Get(ctx, dispatchedKey(id))
	if errors.Is(err, ErrNotFound) {
		return dispatchMarker{}, false, nil
	}
	if err != nil {
		return dispatchMarker{}, false, fmt.Errorf("reading dispatch marker for %s: %w", id, err)
	}
	var m dispatchMarker
	if err := json.Unmarshal(body, &m); err != nil {
		return dispatchMarker{}, false, fmt.Errorf("parsing dispatch marker for %s: %w", id, err)
	}
	return m, true, nil
}

func (s *objectStore) MarkDispatched(ctx context.Context, id string, runID int64, approver string) (Plan, bool, error) {
	if err := checkID(id); err != nil {
		return Plan{}, false, err
	}
	p, err := s.Get(ctx, id)
	if err != nil {
		return Plan{}, false, err
	}
	body, err := json.Marshal(dispatchMarker{RunID: runID, Approver: approver, At: s.now().UTC()})
	if err != nil {
		return Plan{}, false, err
	}
	err = s.objs.Create(ctx, dispatchedKey(id), body)
	switch {
	case err == nil:
		p.DispatchedRunID = runID
		return p, true, nil
	case errors.Is(err, ErrConflict):
		m, ok, err := s.readDispatched(ctx, id)
		if err != nil {
			return Plan{}, false, err
		}
		if !ok {
			return Plan{}, false, fmt.Errorf("dispatch marker for %s vanished after a conflict", id)
		}
		p.DispatchedRunID = m.RunID
		return p, false, nil
	default:
		return Plan{}, false, fmt.Errorf("marking plan %s dispatched: %w", id, err)
	}
}

func (s *objectStore) MarkDone(ctx context.Context, id, outcome string) error {
	if err := checkID(id); err != nil {
		return err
	}
	body, err := json.Marshal(doneMarker{Outcome: outcome, At: s.now().UTC()})
	if err != nil {
		return err
	}
	if err := s.objs.Create(ctx, doneKey(id), body); err != nil && !errors.Is(err, ErrConflict) {
		return fmt.Errorf("marking plan %s done: %w", id, err)
	}
	return nil
}

// dirObjects is Objects over a directory. Writes are serialised within the
// process; it is for local use and tests, not for cross-host locking.
type dirObjects struct {
	root string
	mu   sync.Mutex
}

// NewDirObjects returns Objects stored as files under root.
func NewDirObjects(root string) Objects { return &dirObjects{root: root} }

func dirETag(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (d *dirObjects) path(key string) string { return filepath.Join(d.root, filepath.FromSlash(key)) }

func (d *dirObjects) write(key string, body []byte) error {
	p := d.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), p)
}

func (d *dirObjects) read(key string) ([]byte, error) {
	b, err := os.ReadFile(d.path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return b, err
}

func (d *dirObjects) Create(_ context.Context, key string, body []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, err := d.read(key); err == nil {
		return ErrConflict
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	return d.write(key, body)
}

func (d *dirObjects) Get(_ context.Context, key string) ([]byte, string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	b, err := d.read(key)
	if err != nil {
		return nil, "", err
	}
	return b, dirETag(b), nil
}

func (d *dirObjects) Replace(_ context.Context, key string, body []byte, etag string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, err := d.read(key)
	if errors.Is(err, ErrNotFound) || (err == nil && dirETag(cur) != etag) {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	return d.write(key, body)
}

func (d *dirObjects) Delete(_ context.Context, key, etag string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, err := d.read(key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if dirETag(cur) != etag {
		return ErrConflict
	}
	return os.Remove(d.path(key))
}
