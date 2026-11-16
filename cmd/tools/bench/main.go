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

// Command bench discovers, validates and runs the connector benchmark suites
// under internal/impl, and turns their output into a result JSON document.
//
// Usage:
//
//	go run ./cmd/tools/bench list [--json]
//	go run ./cmd/tools/bench check
//	go run ./cmd/tools/bench run [flags] <suite>
//	go run ./cmd/tools/bench parse < connect.log
//
// See README.md for the suite contract and the result schema.
package main

import (
	"log"
	"os"
)

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "list":
		err = cmdList(os.Args[2:], os.Stdout)
	case "check":
		err = cmdCheck(os.Args[2:], os.Stdout)
	case "run":
		err = cmdRun(os.Args[2:], os.Stdout, os.Stderr)
	case "parse":
		err = cmdParse(os.Args[2:], os.Stdin, os.Stdout)
	case "help", "-h", "--help":
		printUsage()
	default:
		printUsage()
		log.Fatalf("unknown subcommand: %s", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func printUsage() {
	log.Println("Usage: bench <command> [flags]")
	log.Println("")
	log.Println("Commands:")
	log.Println("  list   List every benchmark suite, its prerequisites and its scenarios")
	log.Println("  check  Validate the suites that declare a bench.yaml manifest")
	log.Println("  run    Run one suite scenario and write a result JSON document")
	log.Println("  parse  Read a Connect log on stdin and print its throughput as JSON")
	log.Println("")
	log.Println("Run 'bench <command> -h' for the flags of a command.")
}
