package stream

import (
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
	confBytes, err := os.ReadFile("./foo.yaml")
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
	<-sigChan

	log.Println("Received SIGTERM, the service is closing.")
}
