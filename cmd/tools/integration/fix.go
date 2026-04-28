// Copyright 2026 Redpanda Data, Inc.
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

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/redpanda-data/connect/v4/cmd/tools/integration/llmfix"
)

func cmdFix(args []string) error {
	fset := flag.NewFlagSet("fix", flag.ExitOnError)
	fixTimeout := fset.Duration("fix-timeout", 30*time.Minute, "timeout per fix agent run")

	flags, positional := splitFlagsAndArgs(fset, args)
	if err := fset.Parse(flags); err != nil {
		return err
	}
	positional = append(positional, fset.Args()...)

	if len(positional) != 1 {
		return errors.New("usage: integration fix <output-file.txt>")
	}

	filePath, err := filepath.Abs(positional[0])
	if err != nil {
		return fmt.Errorf("resolving path: %w", err)
	}

	cached := checkCache(filePath)
	if cached.Package == "" {
		return fmt.Errorf("no package found in %s", filePath)
	}
	if cached.Overall() != ResultFail {
		log.Printf("no failures in %s", filePath)
		return nil
	}

	baseSHA, err := resolveHEAD()
	if err != nil {
		return err
	}

	outputDir := filepath.Dir(filePath)
	slug := pkgSlug(cached.Package)
	tag := llmfix.NewTag(slug)

	dir, err := llmfix.CreateWorktree(tag, baseSHA)
	if err != nil {
		return fmt.Errorf("creating worktree: %w", err)
	}
	defer llmfix.CleanupWorktree(dir, tag)

	logPath := filepath.Join(outputDir, tag+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("creating log file: %w", err)
	}
	defer logFile.Close()

	logger := log.New(io.MultiWriter(logFile, os.Stdout), "", log.LstdFlags)

	op := llmfix.NewOperator(llmfix.FixRequest{
		Tag:         tag,
		PkgPath:     cached.Package,
		TestOutput:  dumpTestOutput(filePath, cached.Tests),
		OutputDir:   outputDir,
		WorktreeDir: dir,
		Timeout:     *fixTimeout,
	}, logger)

	if err := op.Run(); err != nil {
		return err
	}

	commits, err := llmfix.CherryPickCommits(dir, baseSHA)
	if err != nil {
		return fmt.Errorf("cherry-pick: %w", err)
	}
	for _, c := range commits {
		logger.Printf("cherry-picked: %s", c)
	}
	return nil
}

func dumpTestOutput(outFile string, tests []CacheEntry) string {
	var buf strings.Builder
	for _, t := range tests {
		if t.Result != ResultFail {
			continue
		}
		buf.WriteString("---\n")
		fmt.Fprintf(&buf, "Test: %s\n", t.TestName)
		fmt.Fprintf(&buf, "Location: %s:%d\n\n", outFile, t.FailLine)

		if t.FailLine > 0 {
			var output bytes.Buffer
			if err := showTestOutput(&output, outFile, t.FailLine); err != nil {
				fmt.Fprintf(&buf, "(failed to extract output: %v)\n", err)
			} else {
				buf.WriteString(output.String())
			}
		}
		buf.WriteString("\n")
	}
	return buf.String()
}
