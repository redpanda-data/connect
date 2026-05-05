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
	"io/fs"
	"log/slog"
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

Bloblang V1 import statements ("import \"./helpers.blobl\"") are followed: the
referenced files are translated to V2 alongside the YAML and emitted at the
same suffix-derived sibling paths (e.g. "./helpers.v5.blobl" for the default
"--suffix=.v5"), with the in-source import statement rewritten to point at
the new file.

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

	// Memo of imports already written across the run, keyed by canonical key.
	// Two YAMLs that share a `.blobl` import would otherwise produce identical
	// writes; the memo avoids the redundant work and the redundant report.
	importsWritten := map[string]struct{}{}

	var failed bool
	for _, target := range targets {
		opts := configmig.Options{
			BloblangMigrator: bloblMig,
			BloblangOptions: bloblmig.Options{
				Verbose:      c.Bool("verbose"),
				FileResolver: bloblangImportResolver(filepath.Dir(target)),
			},
			MinCoverage: c.Float64("min-coverage"),
			Verbose:     c.Bool("verbose"),
		}
		// V2ImportPathRewriter is shared between two callers: the bloblang
		// migrator (rewriting in-source path strings) and the per-file write
		// loop below (deriving on-disk output paths from canonical keys).
		// Pure-lexical suffix splicing satisfies both since canonical keys are
		// just absolute paths and V1 imports are written as relative strings.
		opts.BloblangOptions.V2ImportPathRewriter = importPathRewriter(c)
		opts.BloblangFileResolver = opts.BloblangOptions.FileResolver
		opts.BloblangV2ImportPathRewriter = opts.BloblangOptions.V2ImportPathRewriter

		ok, err := migrateOneFile(c, cfgMig, opts, target, reportFmt, stdout, stderr, importsWritten)
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

// bloblangImportResolver returns a FileResolver suitable for migrate v5. The
// closure is rooted at yamlDir for top-level imports (parentKey empty) and
// follows transitive imports relative to the parent file's directory. Read
// errors that aren't simple "missing file" cases are surfaced via slog so the
// user can distinguish a typoed path from a permissions problem.
func bloblangImportResolver(yamlDir string) bloblmig.FileResolver {
	return func(parentKey, importPath string) (string, string, bool) {
		base := yamlDir
		if parentKey != "" {
			base = filepath.Dir(parentKey)
		}
		var resolved string
		if filepath.IsAbs(importPath) {
			resolved = importPath
		} else {
			resolved = filepath.Join(base, importPath)
		}
		abs, err := filepath.Abs(resolved)
		if err != nil {
			slog.Warn("failed to resolve bloblang import path",
				"import", importPath, "err", err)
			return "", "", false
		}
		body, err := os.ReadFile(abs)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				// Distinguish unreadable from missing — missing imports flow
				// through the migrator as an Unsupported change at the import
				// site (and surface in the per-file report); read errors that
				// aren't ENOENT (permissions, dangling symlinks, etc.) are
				// otherwise silent.
				slog.Warn("failed to read bloblang import",
					"import", importPath, "resolved", abs, "err", err)
			}
			return "", "", false
		}
		return abs, string(body), true
	}
}

// importPathRewriter returns the V2ImportPathRewriter for the run. In
// --in-place mode the rewrite is the identity function so V2-translated
// content overwrites the V1 file at the same path; otherwise the configured
// suffix is spliced before the extension (mirroring the YAML output layout).
func importPathRewriter(c *cli.Context) bloblmig.V2ImportPathRewriter {
	if c.Bool("in-place") {
		return func(p string) string { return p }
	}
	suffix := c.String("suffix")
	return func(p string) string {
		return siblingPath(p, suffix)
	}
}

// migrateOneFile reads, migrates, and (per CLI flags) writes a single config
// file. Returns ok=false when --check found problems or the migration produced
// any OutcomeUnsupported change.
//
// importsWritten is a memo shared across all targets in the run: when two
// YAMLs share a `.blobl` import, the closure is written once and skipped on
// subsequent encounters. Caller passes the same map for every invocation in a
// single migrate v5 run.
func migrateOneFile(c *cli.Context, mig *configmig.Migrator, opts configmig.Options, path, reportFmt string, stdout, stderr io.Writer, importsWritten map[string]struct{}) (bool, error) {
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
		// --check mode: walk the import closure but write nothing. Surface
		// what *would* be written so the user can audit the rewrite plan.
		for canonicalKey := range rep.BloblangV2Files {
			if _, seen := importsWritten[canonicalKey]; seen {
				continue
			}
			importsWritten[canonicalKey] = struct{}{}
			outPath := opts.BloblangV2ImportPathRewriter(canonicalKey)
			fmt.Fprintf(stdout, "  would migrate import: %s -> %s\n", canonicalKey, outPath)
		}
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

	if err := writeImportClosure(c, opts, rep, importsWritten); err != nil {
		return false, err
	}

	return !hasUnsupported, nil
}

// writeImportClosure persists every Bloblang import the migrator pulled into
// its translation closure. In --in-place mode each canonical key is
// overwritten in place (with an optional .bak backup of the V1 content);
// otherwise each file lands at its rewritten sibling path.
//
// Already-written canonical keys are skipped via importsWritten so a `.blobl`
// shared between two YAMLs only produces one set of disk writes per run.
func writeImportClosure(c *cli.Context, opts configmig.Options, rep *configmig.Report, importsWritten map[string]struct{}) error {
	for canonicalKey, v2Source := range rep.BloblangV2Files {
		if _, seen := importsWritten[canonicalKey]; seen {
			continue
		}
		importsWritten[canonicalKey] = struct{}{}

		outPath := opts.BloblangV2ImportPathRewriter(canonicalKey)
		if c.Bool("in-place") && !c.Bool("no-backup") {
			// Back up the V1 content alongside the file we're about to
			// overwrite. Re-read from disk because the migrator returned the
			// V2 content; the V1 source still lives at canonicalKey.
			v1Body, err := os.ReadFile(canonicalKey)
			if err != nil {
				return fmt.Errorf("read import for backup %s: %w", canonicalKey, err)
			}
			if err := os.WriteFile(canonicalKey+".bak", v1Body, 0o644); err != nil {
				return fmt.Errorf("write import backup %s.bak: %w", canonicalKey, err)
			}
		}
		if err := os.WriteFile(outPath, []byte(v2Source), 0o644); err != nil {
			return fmt.Errorf("write migrated import %s: %w", outPath, err)
		}
	}
	return nil
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
