package stream

import (
	"bytes"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// SplitToBatch is a types.Processor implementation that reads a single message
// containing line delimited payloads and splits the payloads into a single
// batch of messages per line.
type SplitToBatch struct{}

// ProcessMessage splits messages of a batch by lines and sends them onwards as
// a batch of messages.
func (p SplitToBatch) ProcessMessage(m types.Message) ([]types.Message, types.Response) {
	var splitParts [][]byte
	m.Iter(func(i int, b types.Part) error {
		splitParts = append(splitParts, bytes.Split(b.Get(), []byte("\n"))...)
		return nil
	})

	return []types.Message{message.New(splitParts)}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p SplitToBatch) CloseAsync() {
	// Do nothing as our processor doesn't require resource cleanup.
}

// WaitForClose blocks until the processor has closed down.
func (p SplitToBatch) WaitForClose(timeout time.Duration) error {
	// Do nothing as our processor doesn't require resource cleanup.
	return nil
}

// ExampleSplitToBatch demonstrates running a Kafka to Kafka stream where each
// incoming message is parsed as a line delimited blob of payloads and the
// payloads are sent on as a single batch of messages.
func Example_splitToBatch() {
	conf := NewConfig()

	conf.Input.Type = input.TypeKafka
	conf.Input.Kafka.Addresses = []string{
		"localhost:9092",
	}
	conf.Input.Kafka.Topic = "example_topic_one"

	conf.Output.Type = output.TypeKafka
	conf.Output.Kafka.Addresses = []string{
		"localhost:9092",
	}
	conf.Output.Kafka.Topic = "example_topic_two"

	s, err := New(conf, OptAddProcessors(func() (types.Processor, error) {
		return SplitToBatch{}, nil
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
