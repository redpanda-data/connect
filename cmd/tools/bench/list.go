// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func cmdList(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "Print the suites as a JSON array")
	_ = fs.Parse(args)

	root, err := repoRoot()
	if err != nil {
		return err
	}
	suites, err := discover(root)
	if err != nil {
		return err
	}
	if *asJSON {
		return writeJSON(stdout, suites)
	}

	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SUITE\tCOMPONENT\tSCENARIOS\tREQUIRES\tDIR")
	for _, s := range suites {
		if !s.Conformant {
			continue
		}
		var scenarios []string
		for _, sc := range s.Scenarios {
			scenarios = append(scenarios, sc.Name)
		}
		requires := append(append([]string{}, s.Requires.Tools...), s.Requires.Env...)
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", s.Name, s.Component,
			strings.Join(scenarios, ","), strings.Join(requires, ","), s.Dir)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	header := false
	for _, s := range suites {
		if s.Conformant {
			continue
		}
		if !header {
			fmt.Fprintln(stdout, "\nNot runnable with 'bench run' yet:")
			header = true
		}
		fmt.Fprintf(stdout, "  %s: %s\n", s.Dir, strings.Join(s.Problems, "; "))
	}
	return nil
}

// cmdCheck fails when a suite that declares a manifest does not honour the
// contract. Suites without a manifest are not checked: they are migrated one
// at a time.
func cmdCheck(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	_ = fs.Parse(args)

	root, err := repoRoot()
	if err != nil {
		return err
	}
	suites, err := discover(root)
	if err != nil {
		return err
	}

	var checked, failed, legacy int
	for _, s := range suites {
		switch {
		case s.Conformant:
			checked++
			fmt.Fprintf(stdout, "ok    %s (%s)\n", s.Name, s.Dir)
		case s.declared:
			checked++
			failed++
			fmt.Fprintf(stdout, "FAIL  %s\n", s.Dir)
			for _, p := range s.Problems {
				fmt.Fprintf(stdout, "        %s\n", p)
			}
		default:
			legacy++
		}
	}
	fmt.Fprintf(stdout, "%d suites checked, %d failed, %d without a manifest\n", checked, failed, legacy)
	if failed > 0 {
		return errors.New("some suites do not honour the benchmark contract, see cmd/tools/bench/README.md")
	}
	return nil
}
