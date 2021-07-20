package integration

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type benchDefinition func(*testing.B, *testEnvironment)

type integrationBenchList []benchDefinition

func integrationBenchs(tests ...benchDefinition) integrationBenchList {
	return tests
}

func (i integrationBenchList) Run(b *testing.B, configTemplate string, opts ...testOptFunc) {
	for _, bench := range i {
		env := newTestEnvironment(b, configTemplate)
		for _, opt := range opts {
			opt(&env)
		}

		if env.preTest != nil {
			env.preTest(b, &env)
		}
		bench(b, &env)
	}
}

var registeredIntegrationBenchmarks = map[string]func(*testing.B){}

// register an integration test that should only execute under the `integration`
// build tag. Returns an empty struct so that it can be called at a file root.
func registerIntegrationBench(name string, fn func(*testing.B)) struct{} {
	if _, exists := registeredIntegrationBenchmarks[name]; exists {
		panic(fmt.Sprintf("integration benchmark double registered: %v", name))
	}
	registeredIntegrationBenchmarks[name] = fn
	return struct{}{}
}

func namedBench(name string, test benchDefinition) benchDefinition {
	return func(b *testing.B, env *testEnvironment) {
		b.Run(name, func(b *testing.B) {
			test(b, env)
		})
	}
}

//------------------------------------------------------------------------------

func integrationBenchSend(batchSize, parallelism int) benchDefinition {
	return namedBench(
		fmt.Sprintf("send message batches %v with parallelism %v", batchSize, parallelism),
		func(b *testing.B, env *testEnvironment) {
			require.Greater(b, parallelism, 0)

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(b, tranChan, env)
			b.Cleanup(func() {
				closeConnectors(b, input, output)
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
					messageInSet(b, true, true, receiveMessage(env.ctx, b, input.TransactionChan(), nil), set)
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

func integrationBenchWrite(batchSize int) benchDefinition {
	return namedBench(
		fmt.Sprintf("write message batches %v without reading", batchSize),
		func(b *testing.B, env *testEnvironment) {
			tranChan := make(chan types.Transaction)
			output := initOutput(b, tranChan, env)
			b.Cleanup(func() {
				closeConnectors(b, nil, output)
			})

			sends := b.N / batchSize

			b.ResetTimer()

			batch := make([]string, batchSize)
			for j := 0; j < sends; j++ {
				for i := 0; i < batchSize; i++ {
					batch[i] = fmt.Sprintf("hello world %v", j*sends+i)
				}
				assert.NoError(b, sendBatch(env.ctx, b, tranChan, batch))
			}
		},
	)
}
