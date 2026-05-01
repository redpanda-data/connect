// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/benthos/v4/public/service"

	bloblmig "github.com/redpanda-data/benthos/v4/public/bloblangv2/migrator"
	configmig "github.com/redpanda-data/benthos/v4/public/service/migrator"

	"github.com/redpanda-data/connect/v4/internal/bloblang/migratorrules"
)

// migrateCli is the parent `migrate` command. It groups every config
// migration that ships in the v4 release line under a single subcommand
// namespace; today only `migrate v5` exists.
func migrateCli(_ *service.ConfigSchema) *cli.Command {
	return &cli.Command{
		Name:  "migrate",
		Usage: "Rewrite {{.ProductName}} configs across major version boundaries",
		Description: `
Migrate one or more {{.ProductName}} stream configuration files to a newer config style.
Subcommands target a specific destination version.`[1:],
		Subcommands: []*cli.Command{
			migrateV5Cli(),
		},
	}
}

// migrateV5Cli rewrites V1 Bloblang plugin callsites in stream configs to the
// V2 plugin set that v5 will ship with. The V1 plugin registrations stay in
// place for the v4 release line; the command opts callers in to the V2 form
// ahead of v5 cutting over.
func migrateV5Cli() *cli.Command {
	return &cli.Command{
		Name:      "v5",
		Usage:     "Rewrite configs to the v5 Bloblang V2 plugin set",
		ArgsUsage: "<file>...",
		Description: `
Rewrites every Bloblang mapping (bloblang/mapping/mutation processors and
in-line bloblang plugin callsites) inside the supplied stream configs to the
V2 plugin equivalents shipped under the same names.

By default a sibling file is written next to each input ({input}.v5.yaml).
Use --in-place to overwrite the input (a .bak backup is written unless
--no-backup is set), or --check to report what would change without writing.`[1:],
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "in-place",
				Aliases: []string{"i"},
				Usage:   "Overwrite the input file. A {input}.bak backup is written unless --no-backup is also set.",
			},
			&cli.StringFlag{
				Name:  "suffix",
				Value: ".v5",
				Usage: "Sibling filename suffix when not in-place (e.g. \".v5\" yields foo.v5.yaml).",
			},
			&cli.BoolFlag{
				Name:  "check",
				Usage: "Report what would change without writing. Exits with status 1 if any plugin instance is unsupported or coverage falls below --min-coverage.",
			},
			&cli.BoolFlag{
				Name:  "verbose",
				Usage: "Surface info-severity changes (e.g. Skip reasons) in the per-file report.",
			},
			&cli.Float64Flag{
				Name:  "min-coverage",
				Value: 0,
				Usage: "Per-file coverage gate (0 disables). The ratio is Rewritten / (Rewritten + Unsupported).",
			},
			&cli.StringFlag{
				Name:  "report",
				Value: "text",
				Usage: "Per-file report format: \"text\" or \"json\".",
			},
			&cli.BoolFlag{
				Name:  "no-backup",
				Usage: "When --in-place is set, suppress the {input}.bak backup file.",
			},
		},
		Action: runMigrateV5,
	}
}

func runMigrateV5(c *cli.Context) error {
	failed, err := runMigrateV5With(c, os.Stdout, os.Stderr)
	if err != nil {
		return err
	}
	if failed {
		os.Exit(1)
	}
	return nil
}

// runMigrateV5With is the testable core of the migrate v5 action. It returns
// (failed, err): err is reserved for fatal flag/argument problems; failed
// flags one-or-more files in which the migration produced an unsupported
// outcome or the coverage gate tripped. The CLI wrapper translates failed=true
// into os.Exit(1) so the test harness can observe both bits independently.
func runMigrateV5With(c *cli.Context, stdout, stderr io.Writer) (bool, error) {
	if c.Bool("check") && c.Bool("in-place") {
		return false, errors.New("--check and --in-place are mutually exclusive")
	}

	reportFmt := strings.ToLower(c.String("report"))
	switch reportFmt {
	case "text", "json":
	default:
		return false, fmt.Errorf("unknown --report format %q (expected \"text\" or \"json\")", reportFmt)
	}

	bloblMig := bloblmig.New()
	migratorrules.Register(bloblMig)

	cfgMig := configmig.New()

	opts := configmig.Options{
		BloblangMigrator: bloblMig,
		BloblangOptions: bloblmig.Options{
			Verbose: c.Bool("verbose"),
		},
		MinCoverage: c.Float64("min-coverage"),
		Verbose:     c.Bool("verbose"),
	}

	args := c.Args().Slice()
	if len(args) == 0 {
		return false, errors.New("at least one config path is required")
	}
	targets, err := service.Globs(service.OSFS(), args...)
	if err != nil {
		return false, err
	}
	if len(targets) == 0 {
		return false, errors.New("no files matched the supplied paths")
	}

	var failed bool
	for _, target := range targets {
		ok, err := migrateOneFile(c, cfgMig, opts, target, reportFmt, stdout, stderr)
		if err != nil {
			fmt.Fprintf(stderr, "%v: %v\n", target, red(err.Error()))
			failed = true
			continue
		}
		if !ok {
			failed = true
		}
	}

	return failed, nil
}

// migrateOneFile reads, migrates, and (per CLI flags) writes a single config
// file. Returns ok=false when --check found problems or the migration produced
// any OutcomeUnsupported change.
func migrateOneFile(c *cli.Context, mig *configmig.Migrator, opts configmig.Options, path, reportFmt string, stdout, stderr io.Writer) (bool, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("read: %w", err)
	}

	rep, err := mig.Migrate(src, opts)
	if err != nil {
		// CoverageError carries a Report; surface it but treat as failure.
		var covErr *configmig.CoverageError
		if errors.As(err, &covErr) {
			emitReport(stderr, path, covErr.Report, reportFmt)
			return false, nil
		}
		return false, err
	}

	emitReport(stdout, path, rep, reportFmt)

	hasUnsupported := rep.Coverage.Unsupported > 0

	switch {
	case c.Bool("check"):
		return !hasUnsupported, nil
	case c.Bool("in-place"):
		if !c.Bool("no-backup") {
			if err := os.WriteFile(path+".bak", src, 0o644); err != nil {
				return false, fmt.Errorf("write backup: %w", err)
			}
		}
		if err := os.WriteFile(path, []byte(rep.OutputYAML), 0o644); err != nil {
			return false, fmt.Errorf("write in place: %w", err)
		}
	default:
		out := siblingPath(path, c.String("suffix"))
		if err := os.WriteFile(out, []byte(rep.OutputYAML), 0o644); err != nil {
			return false, fmt.Errorf("write %v: %w", out, err)
		}
	}

	return !hasUnsupported, nil
}

// siblingPath returns the destination file path for a non-in-place migration.
// `foo.yaml` with suffix `.v5` yields `foo.v5.yaml`. Files without an
// extension get `<path><suffix>` (no extension to splice).
func siblingPath(input, suffix string) string {
	ext := filepath.Ext(input)
	if ext == "" {
		return input + suffix
	}
	return strings.TrimSuffix(input, ext) + suffix + ext
}

// emitReport prints a per-file summary in the requested format. Text format
// is a one-line headline with one indented line per matched component; JSON
// format dumps the Report struct verbatim for tooling consumers.
func emitReport(w io.Writer, path string, rep *configmig.Report, format string) {
	if format == "json" {
		_ = json.NewEncoder(w).Encode(struct {
			File   string            `json:"file"`
			Report *configmig.Report `json:"report"`
		}{File: path, Report: rep})
		return
	}

	cov := rep.Coverage
	headline := fmt.Sprintf(
		"%v: matched=%d rewritten=%d skipped=%d unsupported=%d coverage=%.2f",
		path, cov.Matched, cov.Rewritten, cov.Skipped, cov.Unsupported, cov.Ratio,
	)
	switch {
	case cov.Unsupported > 0:
		fmt.Fprintln(w, red(headline))
	case cov.Matched == 0:
		fmt.Fprintln(w, yellow(headline))
	default:
		fmt.Fprintln(w, green(headline))
	}

	for _, ch := range rep.Changes {
		label := ch.Label
		if label == "" {
			label = ch.Path
		}
		line := fmt.Sprintf("  [%v] %v %v -> %v", ch.Outcome, ch.Target.ComponentType, label, ch.NewName)
		if ch.Reason != "" {
			line += ": " + ch.Reason
		}
		switch ch.Severity {
		case configmig.SeverityError:
			fmt.Fprintln(w, red(line))
		case configmig.SeverityWarning:
			fmt.Fprintln(w, yellow(line))
		default:
			fmt.Fprintln(w, line)
		}
	}
}
