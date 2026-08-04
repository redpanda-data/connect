// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// reservedTablePropertyPrefix namespaces the table properties this connector
// itself depends on (the commit-id idempotency token in snapshot summaries and
// the timestamp-encoding pin, see commitIDProp and
// icebergx.TimestampEncodingProperty). Keys under this prefix must NEVER be
// stripped from commits: e.g. the timestamp-encoding pin is what guarantees a
// table's parquet files all carry one timestamp annotation, so silently
// dropping it would let later writers flip the encoding mid-table. If a
// catalog prohibits one of these keys the commit fails loudly instead.
const reservedTablePropertyPrefix = "redpanda-connect."

// prohibitedKeysRe matches a catalog rejection that names the table property
// keys it refuses, e.g. Databricks Unity Catalog's
//
//	BadRequestException: Malformed request: INVALID_PARAMETER_VALUE:
//	Table properties contain prohibited keys: schema.name-mapping.default
//
// The match is case-insensitive on the "prohibited keys" marker and tolerates
// any prefix text, but REQUIRES a colon introducing the key list — a bare
// "prohibited keys" phrase, or prose like "prohibited keys detected in the
// request", names no keys and must not match (the \b stops the ? from
// backtracking "keys" into "key"+"s"). The captured remainder is tokenised in
// parseProhibitedPropertyKeys.
var prohibitedKeysRe = regexp.MustCompile(`(?i)prohibited\s+keys?\b\s*:\s*(.+)`)

// parseProhibitedPropertyKeys extracts the property keys named by a catalog's
// prohibited-table-property rejection. It returns nil when err does not look
// like such a rejection. The parse is tolerant of surrounding text, case
// differences on the marker, quotes/brackets around the list, trailing prose
// after a key, and sentence-terminating punctuation — but each token must
// still look like a property key (see looksLikePropertyKey), and tokenising
// stops at the first that doesn't, so sentence prose after the list ("Remove
// them, then retry.") is never learned as keys.
func parseProhibitedPropertyKeys(err error) []string {
	if err == nil {
		return nil
	}
	m := prohibitedKeysRe.FindStringSubmatch(err.Error())
	if m == nil {
		return nil
	}
	var keys []string
	for tok := range strings.SplitSeq(m[1], ",") {
		tok = strings.Trim(strings.TrimSpace(tok), "\"'`[]() ")
		// A property key is a run of [A-Za-z0-9._-]; cut the token at the
		// first character outside that set so trailing prose ("a.b (remove
		// them)") does not leak into the key.
		if i := strings.IndexFunc(tok, func(r rune) bool { return !isPropertyKeyRune(r) }); i >= 0 {
			tok = tok[:i]
		}
		// Keys never start or end with a dot; a trailing one is sentence
		// punctuation ("... keys: a.b.").
		tok = strings.Trim(tok, ".")
		if !looksLikePropertyKey(tok) {
			// The comma-separated list has run into sentence prose ("a.b.
			// Remove them, then retry." yields "then"); stop rather than
			// learn prose words as keys.
			break
		}
		keys = append(keys, tok)
	}
	return keys
}

func isPropertyKeyRune(r rune) bool {
	return r == '.' || r == '-' || r == '_' ||
		(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// looksLikePropertyKey reports whether tok plausibly names a table property:
// it must start with a letter and contain at least one dot. Every key this
// stripper exists for (schema.name-mapping.default, write.delete.mode, ...)
// is dotted; requiring the dot is what keeps prose words out of the strip set.
func looksLikePropertyKey(tok string) bool {
	if tok == "" {
		return false
	}
	if c := tok[0]; (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
		return false
	}
	return strings.Contains(tok, ".")
}

// hasReservedPrefix reports whether key falls under
// reservedTablePropertyPrefix, comparing case-insensitively so a catalog that
// case-folds key names in its rejection (e.g. "Redpanda-Connect.…") still
// triggers the loud refuses-to-strip diagnostic instead of a futile retry.
func hasReservedPrefix(key string) bool {
	return len(key) >= len(reservedTablePropertyPrefix) &&
		strings.EqualFold(key[:len(reservedTablePropertyPrefix)], reservedTablePropertyPrefix)
}

// prohibitedKeySet is a concurrency-safe set of catalog-prohibited property
// keys, guarded by its own mutex so it can be SHARED: the router owns one per
// tableEntry and seeds every committer created for that table with it (see
// CommitConfig.ProhibitedKeys and Router.createWriter). Sharing matters
// because writeWithRetry closes the writer on every failure — without it each
// writer generation would start with an empty strip set and burn one rejected
// commit re-learning the same keys (and a catalog naming one key per
// rejection could livelock recreation forever).
type prohibitedKeySet struct {
	mu   sync.RWMutex
	keys map[string]struct{}
}

func newProhibitedKeySet() *prohibitedKeySet {
	return &prohibitedKeySet{keys: map[string]struct{}{}}
}

// add records key, reporting whether it was newly added.
func (s *prohibitedKeySet) add(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[key]; ok {
		return false
	}
	s.keys[key] = struct{}{}
	return true
}

// snapshot returns the current keys; nil when the set is empty.
func (s *prohibitedKeySet) snapshot() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.keys) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.keys))
	for k := range s.keys {
		out = append(out, k)
	}
	return out
}

// propertyStrippingCatalog is a table.CatalogIO wrapper that filters
// catalog-prohibited property keys out of set-properties updates at the commit
// boundary. Some engine-backed catalogs (Databricks Unity Catalog at least)
// reject commits from external clients that set specific reserved table
// properties — e.g. schema.name-mapping.default, which iceberg-go's
// copy-on-write and merge-on-read deletion paths stage defensively whenever a
// table has no name mapping. The committer learns the prohibited keys from the
// catalog's own rejection (see commitLocked) and records them here; the next
// commit attempt then goes through with those keys removed.
//
// Stripping schema.name-mapping.default is safe: every data file this
// connector (and iceberg-go) writes carries Iceberg field IDs in its parquet
// schema, and a name mapping is only a read-time fallback for resolving files
// WITHOUT field IDs — so a table whose files all have IDs never consults it.
// Similarly write.delete.mode is only a writer-side default; the committer
// enforces copy-on-write behaviour in-process regardless of whether the
// property persists (commitOverwrite stages it into each transaction, where it
// steers txn.Delete before the update is stripped at this boundary).
//
// Only set-properties updates are ever filtered — every other update type
// (add-snapshot, set-snapshot-ref, ...) passes through untouched — and keys
// under reservedTablePropertyPrefix are never accepted into the strip set (see
// addProhibitedKey). The wrapper is installed unconditionally at committer
// construction and is a pure pass-through until a key is learned.
type propertyStrippingCatalog struct {
	inner table.CatalogIO

	// strip carries its own lock and may be shared across committers for the
	// same table (see prohibitedKeySet), so learned keys survive writer
	// recreation.
	strip *prohibitedKeySet

	// sentMu guards lastSent: the property keys the most recent CommitTable
	// actually forwarded to the inner catalog. noteProhibitedKeys consults it
	// so only keys we genuinely sent can be learned from a rejection —
	// commits through a committer are serialized (commitMu), so the last call
	// is always the one whose error is being inspected.
	sentMu   sync.Mutex
	lastSent map[string]struct{}
}

// newPropertyStrippingCatalog wraps inner with prohibited-key filtering. A
// nil strip gets a fresh, private key set; passing a shared set persists
// learned keys across committer generations for the same table.
func newPropertyStrippingCatalog(inner table.CatalogIO, strip *prohibitedKeySet) *propertyStrippingCatalog {
	if strip == nil {
		strip = newProhibitedKeySet()
	}
	return &propertyStrippingCatalog{inner: inner, strip: strip}
}

// addProhibitedKey records key for stripping from future commits, reporting
// whether it was newly added. Keys under reservedTablePropertyPrefix
// (compared case-insensitively) are refused (returning false): those carry
// connector semantics that must not be silently dropped — the caller is
// expected to fail loudly instead.
func (p *propertyStrippingCatalog) addProhibitedKey(key string) bool {
	if hasReservedPrefix(key) {
		return false
	}
	return p.strip.add(key)
}

// sentPropertyKey reports whether the most recent CommitTable through this
// wrapper forwarded a set-properties update containing key.
func (p *propertyStrippingCatalog) sentPropertyKey(key string) bool {
	p.sentMu.Lock()
	defer p.sentMu.Unlock()
	_, ok := p.lastSent[key]
	return ok
}

// recordSentPropertyKeys stores the union of property keys in updates'
// set-properties updates as the most recent commit's sent set. Best-effort:
// an update that cannot be introspected is skipped (the same JSON round-trip
// in filterUpdates would have failed the commit first anyway).
func (p *propertyStrippingCatalog) recordSentPropertyKeys(updates []table.Update) {
	sent := map[string]struct{}{}
	for _, u := range updates {
		if u.Action() != table.UpdateSetProperties {
			continue
		}
		raw, err := json.Marshal(u)
		if err != nil {
			continue
		}
		var payload struct {
			Updates iceberg.Properties `json:"updates"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			continue
		}
		for k := range payload.Updates {
			sent[k] = struct{}{}
		}
	}
	p.sentMu.Lock()
	p.lastSent = sent
	p.sentMu.Unlock()
}

// LoadTable delegates to the wrapped catalog. The returned table keeps its
// binding to the inner catalog, which is fine for the two callers that exist:
// iceberg-go's refresh-and-replay retry only reads the fresh table's metadata,
// and the committer rebinds every table it retains (see NewCommitter).
func (p *propertyStrippingCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return p.inner.LoadTable(ctx, ident)
}

// CommitTable filters learned prohibited keys out of set-properties updates,
// then delegates to the wrapped catalog. With an empty strip set the updates
// slice is forwarded untouched.
func (p *propertyStrippingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	filtered, err := p.filterUpdates(updates)
	if err != nil {
		return nil, "", fmt.Errorf("stripping prohibited table properties from commit updates: %w", err)
	}
	// Remember which property keys this commit actually carries, so a
	// rejection naming a key we never sent is not learned (noteProhibitedKeys).
	p.recordSentPropertyKeys(filtered)
	return p.inner.CommitTable(ctx, ident, reqs, filtered)
}

// filterUpdates returns updates with the strip set's keys removed from every
// set-properties update. A set-properties update left with no keys is dropped
// entirely; updates of any other type, and set-properties updates naming no
// stripped key, pass through as the original values. iceberg-go's concrete
// update structs are unexported JSON-tagged action structs, so the property
// map is extracted via a JSON round-trip and rebuilt with the exported
// table.NewSetPropertiesUpdate constructor (same action, same no-op
// PostCommit).
func (p *propertyStrippingCatalog) filterUpdates(updates []table.Update) ([]table.Update, error) {
	strip := p.strip.snapshot()
	if len(strip) == 0 {
		return updates, nil
	}

	out := make([]table.Update, 0, len(updates))
	for _, u := range updates {
		if u.Action() != table.UpdateSetProperties {
			out = append(out, u)
			continue
		}
		raw, err := json.Marshal(u)
		if err != nil {
			return nil, fmt.Errorf("marshaling set-properties update: %w", err)
		}
		var payload struct {
			Updates iceberg.Properties `json:"updates"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, fmt.Errorf("unmarshaling set-properties update: %w", err)
		}
		removed := false
		for _, k := range strip {
			if _, ok := payload.Updates[k]; ok {
				delete(payload.Updates, k)
				removed = true
			}
		}
		switch {
		case !removed:
			out = append(out, u)
		case len(payload.Updates) == 0:
			// The update only set prohibited keys; drop it outright.
		default:
			out = append(out, table.NewSetPropertiesUpdate(payload.Updates))
		}
	}
	return out, nil
}

// rebindTable returns a table identical to tbl but bound to cat, so every
// transaction commit and refresh issued through the returned handle flows
// through cat. This mirrors the table.New rebinding pattern iceberg-go's own
// tests use; tbl.FS is the table's FSysF as a method value.
func rebindTable(tbl *table.Table, cat table.CatalogIO) *table.Table {
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, cat)
}
