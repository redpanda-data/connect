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
// The match is case-insensitive on the "prohibited keys" marker, tolerates any
// prefix text, an optional colon, and captures the remainder of the message
// for tokenising in parseProhibitedPropertyKeys.
var prohibitedKeysRe = regexp.MustCompile(`(?i)prohibited\s+keys?\s*:?\s*(.+)`)

// parseProhibitedPropertyKeys extracts the property keys named by a catalog's
// prohibited-table-property rejection. It returns nil when err does not look
// like such a rejection. The parse is deliberately tolerant: surrounding text,
// case differences on the marker, quotes/brackets around the list, trailing
// prose after a key, and sentence-terminating punctuation are all accepted.
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
		if tok != "" {
			keys = append(keys, tok)
		}
	}
	return keys
}

func isPropertyKeyRune(r rune) bool {
	return r == '.' || r == '-' || r == '_' ||
		(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
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

	mu    sync.RWMutex
	strip map[string]struct{}
}

func newPropertyStrippingCatalog(inner table.CatalogIO) *propertyStrippingCatalog {
	return &propertyStrippingCatalog{inner: inner, strip: map[string]struct{}{}}
}

// addProhibitedKey records key for stripping from future commits, reporting
// whether it was newly added. Keys under reservedTablePropertyPrefix are
// refused (returning false): those carry connector semantics that must not be
// silently dropped — the caller is expected to fail loudly instead.
func (p *propertyStrippingCatalog) addProhibitedKey(key string) bool {
	if strings.HasPrefix(key, reservedTablePropertyPrefix) {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.strip[key]; ok {
		return false
	}
	p.strip[key] = struct{}{}
	return true
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
	p.mu.RLock()
	n := len(p.strip)
	strip := make([]string, 0, n)
	for k := range p.strip {
		strip = append(strip, k)
	}
	p.mu.RUnlock()
	if n == 0 {
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
