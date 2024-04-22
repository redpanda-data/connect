package integration

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// StreamBenchSend benchmarks the speed at which messages are sent over the
// templated output and then subsequently received from the input with a given
// batch size and parallelism.
func StreamBenchSend(batchSize, parallelism int) StreamBenchDefinition {
	return namedBench(
		fmt.Sprintf("send message batches %v with parallelism %v", batchSize, parallelism),
		func(b *testing.B, env *streamTestEnvironment) {
			require.Greater(b, parallelism, 0)

			tranChan := make(chan message.Transaction)
			input, output := initConnectors(b, tranChan, env)
			b.Cleanup(func() {
				closeConnectors(b, env, input, output)
			})

			sends := b.N / batchSize

			set := map[string][]string{}
			for j := 0; j < sends; j++ {
				for i := 0; i < batchSize; i++ {
					payload := fmt.Sprintf("hello world %v", j*sends+i)
					set[payload] = nil
				}
			}

			b.ResetTimer()

			batchChan := make(chan []string)

			var wg sync.WaitGroup
			for k := 0; k < parallelism; k++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						batch, open := <-batchChan
						if !open {
							return
						}
						assert.NoError(b, sendBatch(env.ctx, b, tranChan, batch))
					}
				}()
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				for len(set) > 0 {
					messagesInSet(b, true, true, receiveBatch(env.ctx, b, input.TransactionChan(), nil), set)
				}
			}()

			for j := 0; j < sends; j++ {
				payloads := []string{}
				for i := 0; i < batchSize; i++ {
					payload := fmt.Sprintf("hello world %v", j*sends+i)
					payloads = append(payloads, payload)
				}
				batchChan <- payloads
			}
			close(batchChan)

			wg.Wait()
		},
	)
}

// StreamBenchWrite benchmarks the speed at which messages can be written to the
// output, with no attempt made to consume the written data.
func StreamBenchWrite(batchSize int) StreamBenchDefinition {
	return namedBench(
		fmt.Sprintf("write message batches %v without reading", batchSize),
		func(b *testing.B, env *streamTestEnvironment) {
			tranChan := make(chan message.Transaction)
			output := initOutput(b, tranChan, env)
			b.Cleanup(func() {
				closeConnectors(b, env, nil, output)
			})

			sends := b.N / batchSize

			b.ResetTimer()

			batch := make([]string, batchSize)
			for j := 0; j < sends; j++ {
				for i := 0; i < batchSize; i++ {
					batch[i] = fmt.Sprintf(`{"content":"hello world","id":%v}`, j*sends+i)
				}
				assert.NoError(b, sendBatch(env.ctx, b, tranChan, batch))
			}
		},
	)
}
