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

package pipeline

import (
	"encoding/json"
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Config is a configuration struct for creating parallel processing pipelines.
// The number of resuling parallel processing pipelines will match the number of
// threads specified. Processors are executed on each message in the order that
// they are written.
//
// In order to fully utilise each processing thread you must either have a
// number of parallel inputs that matches or surpasses the number of pipeline
// threads, or use a memory buffer.
type Config struct {
	Threads    int                `json:"threads" yaml:"threads"`
	Processors []processor.Config `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Threads:    1,
		Processors: []processor.Config{},
	}
}

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
func SanitiseConfig(conf Config) (interface{}, error) {
	cBytes, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = json.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	procSlice := []interface{}{}
	for _, proc := range conf.Processors {
		var procSanitised interface{}
		procSanitised, err = processor.SanitiseConfig(proc)
		if err != nil {
			return nil, err
		}
		procSlice = append(procSlice, procSanitised)
	}
	hashMap["processors"] = procSlice

	return hashMap, nil
}

//------------------------------------------------------------------------------

// New creates an input type based on an input configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	processorCtors ...types.ProcessorConstructorFunc,
) (Type, error) {
	procs := 0
	procCtor := func(i *int) (types.Pipeline, error) {
		processors := make([]types.Processor, len(conf.Processors)+len(processorCtors))
		for j, procConf := range conf.Processors {
			prefix := fmt.Sprintf("processor.%v", *i)
			var err error
			processors[j], err = processor.New(procConf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
			if err != nil {
				return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
			}
			*i++
		}
		for j, procCtor := range processorCtors {
			var err error
			processors[j+len(conf.Processors)], err = procCtor()
			if err != nil {
				return nil, fmt.Errorf("failed to create processor: %v", err)
			}
		}
		return NewProcessor(log, stats, processors...), nil
	}
	if conf.Threads <= 1 {
		return procCtor(&procs)
	}
	return NewPool(procCtor, conf.Threads, log, stats)
}

//------------------------------------------------------------------------------
