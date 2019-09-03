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

package stream

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
)

// CustomProcessor is a types.Processor implementation that does nothing.
type CustomProcessor struct{}

// ProcessMessage does nothing.
func (p CustomProcessor) ProcessMessage(m types.Message) ([]types.Message, types.Response) {
	return []types.Message{m}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p CustomProcessor) CloseAsync() {
	// Do nothing as our processor doesn't require resource cleanup.
}

// WaitForClose blocks until the processor has closed down.
func (p CustomProcessor) WaitForClose(timeout time.Duration) error {
	// Do nothing as our processor doesn't require resource cleanup.
	return nil
}

// ExampleYAMLConfig demonstrates running a Benthos stream with a configuration
// parsed from a YAML file and a custom processor.
func Example_yamlConfig() {
	confBytes, err := ioutil.ReadFile("./foo.yaml")
	if err != nil {
		panic(err)
	}

	conf := NewConfig()
	if err = yaml.Unmarshal(confBytes, &conf); err != nil {
		panic(err)
	}

	s, err := New(conf, OptAddProcessors(func() (types.Processor, error) {
		return CustomProcessor{}, nil
	}))
	if err != nil {
		panic(err)
	}

	defer s.Stop(time.Second)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
		log.Println("Received SIGTERM, the service is closing.")
	}
}
