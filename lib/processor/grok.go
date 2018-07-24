// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"fmt"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/trivago/grok"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["grok"] = TypeSpec{
		constructor: NewGrok,
		description: `
Parses a payload by attempting to apply a list of Grok patterns, if a pattern
returns at least one value a resulting structured object is created according to
the chosen output format and will replace the payload. Currently only json is a
valid output format.

This processor respects type hints in the grok patterns, therefore with the
pattern ` + "`%{WORD:first},%{INT:second:int}`" + ` and a payload of ` + "`foo,1`" + `
the resulting payload would be ` + "`{\"first\":\"foo\",\"second\":1}`" + `.`,
	}
}

//------------------------------------------------------------------------------

// GrokConfig contains any configuration for the Grok processor.
type GrokConfig struct {
	Parts       []int    `json:"parts" yaml:"parts"`
	Patterns    []string `json:"patterns" yaml:"patterns"`
	RemoveEmpty bool     `json:"remove_empty_values" yaml:"remove_empty_values"`
	NamedOnly   bool     `json:"named_captures_only" yaml:"named_captures_only"`
	UseDefaults bool     `json:"use_default_patterns" yaml:"use_default_patterns"`
	To          string   `json:"output_format" yaml:"output_format"`
}

// NewGrokConfig returns a GrokConfig with default values.
func NewGrokConfig() GrokConfig {
	return GrokConfig{
		Parts:       []int{},
		Patterns:    []string{},
		RemoveEmpty: true,
		NamedOnly:   true,
		UseDefaults: true,
		To:          "json",
	}
}

//------------------------------------------------------------------------------

// Grok is a processor that executes Grok queries on a message part and
// replaces the contents with the result.
type Grok struct {
	parts    []int
	gparsers []*grok.CompiledGrok

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrGrok   metrics.StatCounter
	mErrJSONS  metrics.StatCounter
	mSucc      metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewGrok returns a Grok processor.
func NewGrok(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	gcompiler, err := grok.New(grok.Config{
		RemoveEmptyValues:   conf.Grok.RemoveEmpty,
		NamedCapturesOnly:   conf.Grok.NamedOnly,
		SkipDefaultPatterns: !conf.Grok.UseDefaults,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create grok compiler: %v", err)
	}

	var compiled []*grok.CompiledGrok
	for _, pattern := range conf.Grok.Patterns {
		var gcompiled *grok.CompiledGrok
		if gcompiled, err = gcompiler.Compile(pattern); err != nil {
			return nil, fmt.Errorf("failed to compile Grok pattern '%v': %v", pattern, err)
		}
		compiled = append(compiled, gcompiled)
	}

	g := &Grok{
		parts:    conf.Grok.Parts,
		gparsers: compiled,
		conf:     conf,
		log:      log.NewModule(".processor.grok"),
		stats:    stats,

		mCount:     stats.GetCounter("processor.grok.count"),
		mErrGrok:   stats.GetCounter("processor.grok.error.grok_no_matches"),
		mErrJSONS:  stats.GetCounter("processor.grok.error.json_set"),
		mSucc:      stats.GetCounter("processor.grok.success"),
		mSent:      stats.GetCounter("processor.grok.sent"),
		mSentParts: stats.GetCounter("processor.grok.parts.sent"),
	}
	return g, nil
}

//------------------------------------------------------------------------------

// ProcessMessage parses message parts as grok patterns.
func (g *Grok) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	g.mCount.Incr(1)

	newMsg := msg.ShallowCopy()

	targetParts := g.parts
	if len(targetParts) == 0 {
		targetParts = make([]int, newMsg.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	for _, index := range targetParts {
		body := msg.Get(index)

		var values map[string]interface{}
		for _, compiler := range g.gparsers {
			var err error
			if values, err = compiler.ParseTyped(body); err != nil {
				g.log.Debugf("Failed to parse body: %v\n", err)
				continue
			}
			if len(values) > 0 {
				break
			}
		}

		if len(values) == 0 {
			g.mErrGrok.Incr(1)
			g.log.Debugf("No matches found for payload: %s\n", body)
			continue
		}

		if err := newMsg.SetJSON(index, values); err != nil {
			g.mErrJSONS.Incr(1)
			g.log.Debugf("Failed to convert grok result into json: %v\n", err)
		} else {
			g.mSucc.Incr(1)
		}
	}

	msgs := [1]types.Message{newMsg}

	g.mSent.Incr(1)
	g.mSentParts.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

//------------------------------------------------------------------------------
