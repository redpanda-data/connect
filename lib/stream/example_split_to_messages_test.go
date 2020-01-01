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

// SplitToMessages is a types.Processor implementation that reads a single
// message containing line delimited payloads and splits the payloads into a
// single message per line.
type SplitToMessages struct{}

// ProcessMessage splits messages of a batch by lines and sends them onwards as
// an individual message per payload.
func (p SplitToMessages) ProcessMessage(m types.Message) ([]types.Message, types.Response) {
	var splitParts [][]byte
	m.Iter(func(i int, b types.Part) error {
		splitParts = append(splitParts, bytes.Split(b.Get(), []byte("\n"))...)
		return nil
	})

	messages := make([]types.Message, len(splitParts))
	for i, part := range splitParts {
		messages[i] = message.New([][]byte{part})
	}
	return messages, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p SplitToMessages) CloseAsync() {
	// Do nothing as our processor doesn't require resource cleanup.
}

// WaitForClose blocks until the processor has closed down.
func (p SplitToMessages) WaitForClose(timeout time.Duration) error {
	// Do nothing as our processor doesn't require resource cleanup.
	return nil
}

// ExampleSplitToMessages demonstrates running a Kafka to Kafka stream where
// each incoming message is parsed as a line delimited blob of payloads and the
// payloads are sent on as a single message per payload.
func Example_splitToMessages() {
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
		return SplitToMessages{}, nil
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
