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
	"fmt"
	"io"

	"github.com/fatih/color"
)

// Output writes colored text to the terminal and plain text to the index file.
type Output struct {
	term  io.Writer
	index io.Writer

	greenPr  *color.Color
	redPr    *color.Color
	yellowPr *color.Color
	dimPr    *color.Color
	boldPr   *color.Color
}

// NewOutput creates a new Output writing to the given terminal and index writers.
func NewOutput(term, index io.Writer) *Output {
	return &Output{
		term:     term,
		index:    index,
		greenPr:  color.New(color.FgGreen),
		redPr:    color.New(color.FgRed),
		yellowPr: color.New(color.FgYellow),
		dimPr:    color.New(color.Faint),
		boldPr:   color.New(color.Bold),
	}
}

// Header writes bold text to the terminal and plain text to the index.
func (o *Output) Header(text string) {
	o.boldPr.Fprintln(o.term, text)
	fmt.Fprintln(o.index, text)
}

// Green writes green text to both terminal and index.
func (o *Output) Green(text string) {
	o.greenPr.Fprintln(o.term, text)
	fmt.Fprintln(o.index, text)
}

// Red writes red text to both terminal and index.
func (o *Output) Red(text string) {
	o.redPr.Fprintln(o.term, text)
	fmt.Fprintln(o.index, text)
}

// Yellow writes yellow text to both terminal and index.
func (o *Output) Yellow(text string) {
	o.yellowPr.Fprintln(o.term, text)
	fmt.Fprintln(o.index, text)
}

// Dim writes dim/faint text to both terminal and index.
func (o *Output) Dim(text string) {
	o.dimPr.Fprintln(o.term, text)
	fmt.Fprintln(o.index, text)
}

// Blank writes an empty line to both terminal and index.
func (o *Output) Blank() {
	fmt.Fprintln(o.term)
	fmt.Fprintln(o.index)
}

// Summary writes a colored summary line to the terminal and plain text to the index.
func (o *Output) Summary(passed, failed, skipped int) {
	o.boldPr.Fprint(o.term, "Results: ")
	o.greenPr.Fprintf(o.term, "%d passed", passed)
	o.boldPr.Fprint(o.term, ", ")
	o.redPr.Fprintf(o.term, "%d failed", failed)
	o.boldPr.Fprint(o.term, ", ")
	o.yellowPr.Fprintf(o.term, "%d skipped", skipped)
	fmt.Fprintln(o.term)
	fmt.Fprintf(o.index, "Results: %d passed, %d failed, %d skipped\n", passed, failed, skipped)
}

// OpenPackage writes a bold header to the terminal and an opening XML-like tag to the index.
func (o *Output) OpenPackage(name string, num, total int, short string) {
	o.boldPr.Fprintf(o.term, "[%d/%d] %s\n", num, total, short)
	fmt.Fprintf(o.index, "<%s>\n", name)
}

// ClosePackage writes a closing XML-like tag to the index file only.
func (o *Output) ClosePackage(name string) {
	fmt.Fprintf(o.index, "</%s>\n", name)
	o.FlushIndex()
}

// FlushIndex syncs the index writer if it supports it.
func (o *Output) FlushIndex() {
	type syncer interface {
		Sync() error
	}
	if s, ok := o.index.(syncer); ok {
		if err := s.Sync(); err != nil {
			fmt.Fprintf(o.term, "warning: failed to sync index file: %v\n", err)
		}
	}
}
