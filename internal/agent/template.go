/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package agent

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

//go:embed template/*
var embeddedTemplate embed.FS

// CreateTemplate generates the agent SDK template for RPCN.
func CreateTemplate(dir string, vars map[string]string) error {
	err := unpackEmbeddedFS(embeddedTemplate, "template", dir, vars)
	if err != nil {
		return fmt.Errorf("failed to generate template: %w", err)
	}
	return nil
}

func unpackEmbeddedFS(efs embed.FS, embedPath, destPath string, vars map[string]string) error {
	if err := os.MkdirAll(destPath, os.ModePerm); err != nil {
		return err
	}
	oldnew := []string{}
	for k, v := range vars {
		oldnew = append(oldnew, k, v)
	}
	replacer := strings.NewReplacer(oldnew...)
	return fs.WalkDir(efs, embedPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(embedPath, path)
		if err != nil {
			return err
		}
		outputPath := filepath.Join(destPath, relPath)
		if d.IsDir() {
			return os.MkdirAll(outputPath, os.ModePerm)
		}
		data, err := efs.ReadFile(path)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return err
		}
		_, err = replacer.WriteString(f, string(data))
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
		return err
	})
}
