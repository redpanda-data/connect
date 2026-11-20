// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const keyPrefix = "catalog/v1/"

// Store persists catalogue entries.
type Store interface {
	// Put validates the entry and writes it at Key(e), replacing any
	// existing entry with the same key.
	Put(ctx context.Context, e Entry) error
	// List returns every readable entry for the given input component, or
	// for all inputs when input is empty. Entries with an unknown schema
	// version are skipped; undecodable entries are an error.
	List(ctx context.Context, input string) ([]Entry, error)
}

// Key returns the object key for an entry: "catalog/v1/<input>/<id>.json".
func Key(e Entry) string {
	return keyPrefix + e.Input + "/" + e.ID + ".json"
}

// listPrefix returns the key prefix covering the given input ("" = all).
func listPrefix(input string) string {
	if input == "" {
		return keyPrefix
	}
	return keyPrefix + input + "/"
}

// checkPut validates an entry and its key components, returning the encoded
// body. Input and ID become path segments, so separators are refused.
func checkPut(e Entry) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("invalid catalog entry: %w", err)
	}
	for name, v := range map[string]string{"input": e.Input, "id": e.ID} {
		if strings.ContainsAny(v, `/\`) || v == "." || v == ".." {
			return nil, fmt.Errorf("invalid catalog entry: %s %q must not contain path separators", name, v)
		}
	}
	body, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encoding catalog entry %s: %w", e.ID, err)
	}
	return body, nil
}

// decodeEntry decodes one stored object. It returns ok=false (and no error)
// for entries written under a different schema version.
func decodeEntry(key string, body []byte) (e Entry, ok bool, err error) {
	var hdr struct {
		SchemaVersion int `json:"schema_version"`
	}
	if err := json.Unmarshal(body, &hdr); err != nil {
		return Entry{}, false, fmt.Errorf("decoding catalog object %s: %w", key, err)
	}
	if hdr.SchemaVersion != SchemaVersion {
		return Entry{}, false, nil
	}
	if err := json.Unmarshal(body, &e); err != nil {
		return Entry{}, false, fmt.Errorf("decoding catalog object %s: %w", key, err)
	}
	return e, true, nil
}

type dirStore struct{ root string }

// NewDirStore returns a Store backed by a local directory, laid out exactly
// like the S3 keyspace.
func NewDirStore(root string) Store { return &dirStore{root: root} }

func (d *dirStore) Put(_ context.Context, e Entry) error {
	body, err := checkPut(e)
	if err != nil {
		return err
	}
	path := filepath.Join(d.root, filepath.FromSlash(Key(e)))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	// Write-then-rename so a concurrent List never sees a partial file.
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
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
	return os.Rename(tmp.Name(), path)
}

func (d *dirStore) List(ctx context.Context, input string) ([]Entry, error) {
	base := filepath.Join(d.root, filepath.FromSlash(listPrefix(input)))
	var out []Entry
	err := filepath.WalkDir(base, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if de.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		e, ok, err := decodeEntry(path, body)
		if err != nil {
			return err
		}
		if ok {
			out = append(out, e)
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return out, err
}
