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
	"sync"
	"time"

	"github.com/fatih/color"
)

// Output writes colored text to the terminal and markdown to an index file.
// The run writes its header and summary with Output, and each package writes
// its results with a PackageOutput from Output.Package.
type Output struct {
	// mu makes sure that the text of one package is written to the terminal
	// and the index file as one block.
	mu        sync.Mutex
	term      io.Writer
	indexPath string

	greenPr  *color.Color
	redPr    *color.Color
	yellowPr *color.Color
	dimPr    *color.Color
	boldPr   *color.Color
}

// NewOutput creates a new Output.
func NewOutput(term io.Writer, indexPath string) *Output {
	if os.Getenv("FORCE_COLOR") != "" || os.Getenv("CI") != "" {
		color.NoColor = false
	}
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

func (o *Output) Blank() {
	fmt.Fprintln(o.term)
}

// PackageOutput collects the results of one package. Its markdown goes to the
// index file as one section when the package is done. Only the goroutine that
// runs the package uses it.
type PackageOutput struct {
	o *Output
	// term is where the terminal text goes: the terminal itself for a
	// streamed package, termBuf for a buffered package.
	term io.Writer
	// termBuf holds the terminal text of a buffered package until the package
	// is done. It is nil for a streamed package.
	termBuf *bytes.Buffer
	md      bytes.Buffer
}

// Package starts the output of one package and writes its header. A streamed
// package writes to the terminal at once, which is only readable when one
// package runs at a time. A buffered package writes its terminal text as one
// block when it is done, so packages that run at the same time do not mix
// their lines.
func (o *Output) Package(tag string, num, total int, short string, buffered bool) *PackageOutput {
	p := &PackageOutput{o: o, term: o.term}
	if buffered {
		p.termBuf = &bytes.Buffer{}
		p.term = p.termBuf
	}
	o.boldPr.Fprintf(p.term, "[%d/%d] %s\n", num, total, short)
	fmt.Fprintf(&p.md, "## %s\n\n", tag)
	return p
}

func (p *PackageOutput) Info(text string) {
	p.o.dimPr.Fprintln(p.term, text)
}

func (p *PackageOutput) Error(text string) {
	p.o.redPr.Fprintln(p.term, text)
}

func (p *PackageOutput) TestRun(name string) {
	p.o.dimPr.Fprintln(p.term, "  RUN  "+name)
}

// testLine writes a single test result line to both terminal and markdown buffer.
func (p *PackageOutput) testLine(clr *color.Color, status, name, detail string) {
	line := fmt.Sprintf("  %s %s", status, name)
	mdLine := fmt.Sprintf("- %s %s", status, name)
	if detail != "" {
		line += " " + detail
		mdLine += " " + detail
	}
	clr.Fprintln(p.term, line)
	fmt.Fprintln(&p.md, mdLine)
}

func (p *PackageOutput) TestPass(name, elapsed string, cached bool) {
	if cached {
		p.testLine(p.o.dimPr, "PASS", name, "(cached)")
	} else {
		p.testLine(p.o.greenPr, "PASS", name, "("+elapsed+")")
	}
}

func (p *PackageOutput) TestFail(name, elapsed, outFile string, line int, cached bool) {
	base := filepath.Base(outFile)
	clr := p.o.redPr
	suffix := "(" + elapsed + ")"
	if cached {
		clr = p.o.dimPr
		suffix = "(cached)"
	}
	ref := fmt.Sprintf("%s:%d", outFile, line)
	p.testLine(clr, "FAIL", name, suffix+" -> task test:show -- "+ref)
	// Replace the markdown line with a clickable link.
	p.replaceLastMDLine(fmt.Sprintf("- FAIL %s %s — [%s:%d](%s:%d)",
		name, suffix, base, line, base, line))
}

func (p *PackageOutput) TestSkip(name, elapsed, reason string, cached bool) {
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
		p.testLine(p.o.dimPr, "SKIP", name, detail)
	} else {
		p.testLine(p.o.yellowPr, "SKIP", name, detail)
	}
}

// replaceLastMDLine replaces the last line in md with replacement.
func (p *PackageOutput) replaceLastMDLine(replacement string) {
	data := p.md.Bytes()
	// Find the second-to-last newline.
	if idx := bytes.LastIndexByte(data[:len(data)-1], '\n'); idx >= 0 {
		p.md.Truncate(idx + 1)
	} else {
		p.md.Reset()
	}
	fmt.Fprintln(&p.md, replacement)
}

// Result writes the package result and ends the package output.
func (p *PackageOutput) Result(short string, status TestResult, duration time.Duration) {
	switch status {
	case ResultPass:
		p.o.greenPr.Fprintf(p.term, "✓ %s%s\n", short, fmtDuration(duration))
	case ResultFail:
		p.o.redPr.Fprintf(p.term, "✗ %s%s\n", short, fmtDuration(duration))
	case ResultSkip:
		p.o.yellowPr.Fprintf(p.term, "⊘ %s (skipped)\n", short)
	}
	p.Done()
}

// Done ends the package output without a result line.
func (p *PackageOutput) Done() {
	fmt.Fprintln(p.term)
	fmt.Fprintln(&p.md)
	p.o.writePackage(p)
}

// writePackage writes the buffered terminal text and the markdown section of
// a package that is done.
func (o *Output) writePackage(p *PackageOutput) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if p.termBuf != nil {
		if _, err := o.term.Write(p.termBuf.Bytes()); err != nil {
			log.Fatal("failed to write package results to terminal: ", err)
		}
	}
	o.writeFile(os.O_WRONLY|os.O_APPEND, func(f *os.File) {
		if _, err := f.Write(p.md.Bytes()); err != nil {
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
