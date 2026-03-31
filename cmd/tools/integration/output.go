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
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/color"
)

// Output writes colored text to the terminal and markdown to an index file.
type Output struct {
	term      io.Writer
	indexPath string
	buf       bytes.Buffer

	greenPr  *color.Color
	redPr    *color.Color
	yellowPr *color.Color
	dimPr    *color.Color
	boldPr   *color.Color
}

// NewOutput creates a new Output.
func NewOutput(term io.Writer, indexPath string) *Output {
	return &Output{
		term:      term,
		indexPath: indexPath,
		greenPr:   color.New(color.FgGreen),
		redPr:     color.New(color.FgRed),
		yellowPr:  color.New(color.FgYellow),
		dimPr:     color.New(color.Faint),
		boldPr:    color.New(color.Bold),
	}
}

// Header writes bold text to the terminal and a markdown heading to the index file.
func (o *Output) Header(text string) {
	o.boldPr.Fprintln(o.term, text)
	o.writeFile(os.O_CREATE|os.O_WRONLY|os.O_TRUNC, func(f *os.File) {
		fmt.Fprintf(f, "# %s\n\n", text)
	})
}

func (o *Output) Info(text string) {
	o.dimPr.Fprintln(o.term, text)
}

func (o *Output) Error(text string) {
	o.redPr.Fprintln(o.term, text)
}

func (o *Output) Blank() {
	fmt.Fprintln(o.term)
}

func (o *Output) PackageHeader(tag string, num, total int, short string) {
	o.boldPr.Fprintf(o.term, "[%d/%d] %s\n", num, total, short)
	o.buf.Reset()
	fmt.Fprintf(&o.buf, "## %s\n\n", tag)
}

func (o *Output) TestRun(name string) {
	o.dimPr.Fprintln(o.term, "  RUN  "+name)
}

// testLine writes a single test result line to both terminal and markdown buffer.
func (o *Output) testLine(clr *color.Color, status, name, detail string) {
	line := fmt.Sprintf("  %s %s", status, name)
	mdLine := fmt.Sprintf("- %s %s", status, name)
	if detail != "" {
		line += " " + detail
		mdLine += " " + detail
	}
	clr.Fprintln(o.term, line)
	fmt.Fprintln(&o.buf, mdLine)
}

func (o *Output) TestPass(name, elapsed string, cached bool) {
	if cached {
		o.testLine(o.dimPr, "PASS", name, "(cached)")
	} else {
		o.testLine(o.greenPr, "PASS", name, "("+elapsed+")")
	}
}

func (o *Output) TestFail(name, elapsed, outFile string, line int, cached bool) {
	base := filepath.Base(outFile)
	clr := o.redPr
	suffix := "(" + elapsed + ")"
	if cached {
		clr = o.dimPr
		suffix = "(cached)"
	}
	ref := fmt.Sprintf("%s:%d", outFile, line)
	o.testLine(clr, "FAIL", name, suffix+" -> task test:show -- "+ref)
	// Replace the markdown line with a clickable link.
	o.replaceLastBufLine(fmt.Sprintf("- FAIL %s %s — [%s:%d](%s:%d)",
		name, suffix, base, line, base, line))
}

func (o *Output) TestSkip(name, elapsed, reason string, cached bool) {
	var detail string
	switch {
	case cached && reason != "":
		detail = "(cached): " + reason
	case cached:
		detail = "(cached)"
	case reason != "":
		detail = "(" + elapsed + "): " + reason
	default:
		detail = "(" + elapsed + ")"
	}

	if cached {
		o.testLine(o.dimPr, "SKIP", name, detail)
	} else {
		o.testLine(o.yellowPr, "SKIP", name, detail)
	}
}

// replaceLastBufLine replaces the last line in buf with replacement.
func (o *Output) replaceLastBufLine(replacement string) {
	data := o.buf.Bytes()
	// Find the second-to-last newline.
	if idx := bytes.LastIndexByte(data[:len(data)-1], '\n'); idx >= 0 {
		o.buf.Truncate(idx + 1)
	} else {
		o.buf.Reset()
	}
	fmt.Fprintln(&o.buf, replacement)
}

func (o *Output) PackageResult(short string, status TestResult, duration time.Duration) {
	switch status {
	case ResultPass:
		o.greenPr.Fprintf(o.term, "✓ %s%s\n", short, fmtDuration(duration))
	case ResultFail:
		o.redPr.Fprintf(o.term, "✗ %s%s\n", short, fmtDuration(duration))
	case ResultSkip:
		o.yellowPr.Fprintf(o.term, "⊘ %s (skipped)\n", short)
	}
	fmt.Fprintln(o.term)
	o.flushPackage()
}

func (o *Output) PackageDone() {
	fmt.Fprintln(o.term)
	o.flushPackage()
}

func (o *Output) flushPackage() {
	o.writeFile(os.O_WRONLY|os.O_APPEND, func(f *os.File) {
		fmt.Fprintln(&o.buf)
		_, err := f.Write(o.buf.Bytes())
		if err != nil {
			log.Fatal("failed to write package results to index file: ", err)
		}
	})
}

func (o *Output) Summary(passed, failed, skipped int) {
	o.boldPr.Fprint(o.term, "Results: ")
	o.greenPr.Fprintf(o.term, "%d passed", passed)
	o.boldPr.Fprint(o.term, ", ")
	o.redPr.Fprintf(o.term, "%d failed", failed)
	o.boldPr.Fprint(o.term, ", ")
	o.yellowPr.Fprintf(o.term, "%d skipped", skipped)
	fmt.Fprintln(o.term)

	o.writeFile(os.O_WRONLY|os.O_APPEND, func(f *os.File) {
		fmt.Fprintf(f, "---\n\nResults: %d passed, %d failed, %d skipped\n", passed, failed, skipped)
	})
}

func (o *Output) writeFile(flags int, fn func(f *os.File)) {
	f, err := os.OpenFile(o.indexPath, flags, 0o644)
	if err != nil {
		fmt.Fprintf(o.term, "warning: failed to open index file: %v\n", err)
		return
	}
	defer f.Close()
	fn(f)
}
