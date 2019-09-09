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
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
	"github.com/quipo/dependencysolver"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeProcessDAG] = TypeSpec{
		constructor: NewProcessDAG,
		description: `
A processor that manages a map of ` + "`process_map`" + ` processors and
calculates a Directed Acyclic Graph (DAG) of their dependencies by referring to
their postmap targets for provided fields and their premap targets for required
fields.

The names of workflow stages may only contain alphanumeric, underscore and dash
characters (they must match the regular expression ` + "`[a-zA-Z0-9_-]+`" + `).

The DAG is then used to execute the children in the necessary order with the
maximum parallelism possible. You can read more about workflows in Benthos
[in this document](../workflows.md).

The field ` + "`dependencies`" + ` is an optional array of fields that a child
depends on. This is useful for when fields are required but don't appear within
a premap such as those used in conditions.

This processor is extremely useful for performing a complex mesh of enrichments
where network requests mean we desire maximum parallelism across those
enrichments.

For example, if we had three target HTTP services that we wished to enrich each
document with - foo, bar and baz - where baz relies on the result of both foo
and bar, we might express that relationship here like so:

` + "``` yaml" + `
process_dag:
  foo:
    premap:
      .: .
    processors:
    - http:
        request:
          url: http://foo/enrich
    postmap:
      foo_result: .
  bar:
    premap:
      .: msg.sub.path
    processors:
    - http:
        request:
          url: http://bar/enrich
    postmap:
      bar_result: .
  baz:
    premap:
      foo_obj: foo_result
      bar_obj: bar_result
    processors:
    - http:
        request:
          url: http://baz/enrich
    postmap:
      baz_obj: .
` + "```" + `

With this config the DAG would determine that the children foo and bar can be
executed in parallel, and once they are both finished we may proceed onto baz.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			sanitChildren := map[string]interface{}{}
			for k, v := range conf.ProcessDAG {
				sanit, err := v.Sanitise()
				if err != nil {
					return nil, err
				}
				sanit["dependencies"] = v.Dependencies
				sanitChildren[k] = sanit
			}
			return sanitChildren, nil
		},
	}
}

//------------------------------------------------------------------------------

// DAGDepsConfig is a config containing dependency based configuration values
// for a ProcessDAG child.
type DAGDepsConfig struct {
	Dependencies []string `json:"dependencies" yaml:"dependencies"`
}

// NewDAGDepsConfig returns a default DAGDepsConfig.
func NewDAGDepsConfig() DAGDepsConfig {
	return DAGDepsConfig{
		Dependencies: []string{},
	}
}

// UnmarshalJSON ensures that when parsing configs that are in a slice the
// default values are still applied.
func (p *DAGDepsConfig) UnmarshalJSON(bytes []byte) error {
	type confAlias DAGDepsConfig
	aliased := confAlias(NewDAGDepsConfig())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*p = DAGDepsConfig(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (p *DAGDepsConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias DAGDepsConfig
	aliased := confAlias(NewDAGDepsConfig())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*p = DAGDepsConfig(aliased)
	return nil
}

// DepProcessMapConfig contains a superset of a ProcessMap config and some DAG
// specific fields.
type DepProcessMapConfig struct {
	DAGDepsConfig    `json:",inline" yaml:",inline"`
	ProcessMapConfig `json:",inline" yaml:",inline"`
}

// NewDepProcessMapConfig returns a default DepProcessMapConfig.
func NewDepProcessMapConfig() DepProcessMapConfig {
	return DepProcessMapConfig{
		DAGDepsConfig:    NewDAGDepsConfig(),
		ProcessMapConfig: NewProcessMapConfig(),
	}
}

//------------------------------------------------------------------------------

// ProcessDAGConfig is a config struct containing fields for the
// ProcessDAG processor.
type ProcessDAGConfig map[string]DepProcessMapConfig

// NewProcessDAGConfig returns a default ProcessDAGConfig.
func NewProcessDAGConfig() ProcessDAGConfig {
	return ProcessDAGConfig{}
}

//------------------------------------------------------------------------------

// ProcessDAG is a processor that applies a list of child processors to a new
// payload mapped from the original, and after processing attempts to overlay
// the results back onto the original payloads according to more mappings.
type ProcessDAG struct {
	children map[string]*ProcessMap
	dag      [][]string

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

var processDAGStageName = regexp.MustCompile("[a-zA-Z0-9-_]+")

// NewProcessDAG returns a ProcessField processor.
func NewProcessDAG(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	children := map[string]*ProcessMap{}
	explicitDeps := map[string][]string{}

	for k, v := range conf.ProcessDAG {
		if len(processDAGStageName.FindString(k)) != len(k) {
			return nil, fmt.Errorf("workflow stage name '%v' contains invalid characters", k)
		}

		nsLog := log.NewModule(fmt.Sprintf(".%v", k))
		nsStats := metrics.Namespaced(stats, k)

		child, err := NewProcessMap(v.ProcessMapConfig, mgr, nsLog, nsStats)
		if err != nil {
			return nil, fmt.Errorf("failed to create child process_map '%v': %v", k, err)
		}

		children[k] = child
		explicitDeps[k] = v.Dependencies
	}

	dag, err := resolveDAG(explicitDeps, children)
	if err != nil {
		return nil, err
	}

	p := &ProcessDAG{
		children: children,
		dag:      dag,

		log: log,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	p.log.Infof("Resolved DAG: %v\n", p.dag)
	return p, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *ProcessDAG) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	result := msg.DeepCopy()
	result.Iter(func(i int, p types.Part) error {
		_ = p.Get()
		_, _ = p.JSON()
		_ = p.Metadata()
		return nil
	})

	propMsg, propSpans := tracing.WithChildSpans(TypeProcessDAG, result)

	for _, layer := range p.dag {
		results := make([]types.Message, len(layer))
		errors := make([]error, len(layer))

		wg := sync.WaitGroup{}
		wg.Add(len(layer))
		for i, eid := range layer {
			go func(id string, index int) {
				var resSpans []opentracing.Span
				results[index], resSpans = tracing.WithChildSpans(id, propMsg.Copy())
				errors[index] = p.children[id].CreateResult(results[index])
				for _, s := range resSpans {
					s.Finish()
				}
				wg.Done()
			}(eid, i)
		}
		wg.Wait()

		for i, id := range layer {
			if err := errors[i]; err != nil {
				p.log.Errorf("Failed to perform child '%v': %v\n", id, err)
				result.Iter(func(i int, p types.Part) error {
					FlagErr(p, err)
					return nil
				})
				continue
			}
			if failed, err := p.children[id].OverlayResult(result, results[i]); err != nil {
				p.log.Errorf("Failed to overlay child '%v': %v\n", id, err)
				result.Iter(func(i int, p types.Part) error {
					FlagErr(p, err)
					return nil
				})
				continue
			} else {
				for _, j := range failed {
					FlagErr(result.Get(j), fmt.Errorf("enrichment '%v' postmap failed", id))
				}
			}
		}
	}

	for _, s := range propSpans {
		s.Finish()
	}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(result.Len()))

	msgs := [1]types.Message{result}
	return msgs[:], nil
}

//------------------------------------------------------------------------------

func getDeps(id string, wanted []string, procs map[string]*ProcessMap) []string {
	dependencies := []string{}
	targetsNeeded := wanted

eLoop:
	for k, v := range procs {
		if k == id {
			continue
		}
		for _, tp := range v.TargetsProvided() {
			for _, tn := range targetsNeeded {
				if strings.HasPrefix(tn, tp) {
					dependencies = append(dependencies, k)
					continue eLoop
				}
			}
		}
	}

	return dependencies
}

func resolveDAG(explicitDeps map[string][]string, procs map[string]*ProcessMap) ([][]string, error) {
	if procs == nil || len(procs) == 0 {
		return [][]string{}, nil
	}
	targetProcs := map[string]struct{}{}

	var entries []dependencysolver.Entry
	for id, e := range procs {
		wanted := explicitDeps[id]
		wanted = append(wanted, e.TargetsUsed()...)

		targetProcs[id] = struct{}{}
		entries = append(entries, dependencysolver.Entry{
			ID: id, Deps: getDeps(id, wanted, procs),
		})
	}
	layers := dependencysolver.LayeredTopologicalSort(entries)
	for _, l := range layers {
		for _, id := range l {
			delete(targetProcs, id)
		}
	}
	if len(targetProcs) > 0 {
		var tProcs []string
		for k := range targetProcs {
			tProcs = append(tProcs, k)
		}
		return nil, fmt.Errorf("failed to resolve DAG, circular dependencies detected for targets: %v", tProcs)
	}
	return layers, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *ProcessDAG) CloseAsync() {
	for _, c := range p.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *ProcessDAG) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
