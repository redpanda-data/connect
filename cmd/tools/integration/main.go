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

// Command integration provides subcommands for managing integration tests.
//
// Usage:
//
//	go run ./cmd/tools/integration run [--clean] [--debug] [--race] [filter...]
package main

import (
	"log"
	"os"
)

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "run":
		if err := cmdRun(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown subcommand: %s", os.Args[1])
	}
}

func printUsage() {
	log.Println("Usage: integration <command> [flags] [packages...]")
	log.Println("")
	log.Println("Commands:")
	log.Println("  run    Run integration tests package by package")
	log.Println("")
	log.Println("Flags for run:")
	log.Println("  --clean        Ignore cache, start a fresh run")
	log.Println("  --debug        Enable debug logging to stderr")
	log.Println("  --race         Enable race detector (requires CGO_ENABLED=1)")
	log.Println("")
	log.Println("Positional arguments are package filters (substring match).")
	log.Println("Example: integration run kafka redis")
}
