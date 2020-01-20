package main

import (
	"bytes"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/service"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// HowSarcastic totally detects sarcasm every time.
func HowSarcastic(content []byte) float64 {
	if bytes.Contains(bytes.ToLower(content), []byte("/s")) {
		return 100
	}
	return 0
}

// SarcasmProc applies our sarcasm detector to messages.
type SarcasmProc struct {
	MetadataKey string `json:"metadata_key" yaml:"metadata_key"`
}

// ProcessMessage returns messages mutated with their sarcasm level.
func (s *SarcasmProc) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	newMsg := msg.Copy()

	newMsg.Iter(func(i int, p types.Part) error {
		sarcasm := HowSarcastic(p.Get())
		sarcasmStr := strconv.FormatFloat(sarcasm, 'f', -1, 64)

		if len(s.MetadataKey) > 0 {
			p.Metadata().Set(s.MetadataKey, sarcasmStr)
		} else {
			p.Set([]byte(sarcasmStr))
		}
		return nil
	})

	return []types.Message{newMsg}, nil
}

// CloseAsync does nothing.
func (s *SarcasmProc) CloseAsync() {}

// WaitForClose does nothing.
func (s *SarcasmProc) WaitForClose(timeout time.Duration) error {
	return nil
}

func main() {
	processor.RegisterPlugin(
		"how_sarcastic",
		func() interface{} {
			s := SarcasmProc{}
			return &s
		},
		func(
			iconf interface{},
			mgr types.Manager,
			logger log.Modular,
			stats metrics.Type,
		) (types.Processor, error) {
			return iconf.(*SarcasmProc), nil
		},
	)

	service.Run()
}
