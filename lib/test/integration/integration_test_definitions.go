package integration

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func integrationTestOpenClose() testDefinition {
	return namedTest(
		"can open and close",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			require.NoError(t, sendMessage(env.ctx, t, tranChan, "hello world"))
			messageMatch(t, receiveMessage(env.ctx, t, input.TransactionChan(), nil), "hello world")
		},
	)
}

// The input is created after the output has written data.
func integrationTestOpenCloseIsolated() testDefinition {
	return namedTest(
		"can open and close isolated",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})
			require.NoError(t, sendMessage(env.ctx, t, tranChan, "hello world"))

			input := initInput(t, env)
			t.Cleanup(func() {
				closeConnectors(t, input, nil)
			})
			messageMatch(t, receiveMessage(env.ctx, t, input.TransactionChan(), nil), "hello world")
		},
	)
}

func integrationTestMetadata() testDefinition {
	return namedTest(
		"can send and receive metadata",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			require.NoError(t, sendMessage(
				env.ctx, t, tranChan,
				"hello world",
				"foo", "foo_value",
				"bar", "bar_value",
			))
			messageMatch(
				t, receiveMessage(env.ctx, t, input.TransactionChan(), nil),
				"hello world",
				"foo", "foo_value",
				"bar", "bar_value",
			)
		},
	)
}

func integrationTestSendBatch(n int) testDefinition {
	return namedTest(
		"can send a message batch",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}
			payloads := []string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world %v", i)
				set[payload] = nil
				payloads = append(payloads, payload)
			}
			sendBatch(env.ctx, t, tranChan, payloads)

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

func integrationTestSendBatchCount(n int) testDefinition {
	return namedTest(
		"can send messages with an output batch count",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			env.configVars.outputBatchCount = n

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			resChan := make(chan types.Response)

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world %v", i)
				set[payload] = nil
				msg := message.New(nil)
				msg.Append(message.NewPart([]byte(payload)))
				select {
				case tranChan <- types.NewTransaction(msg, resChan):
				case res := <-resChan:
					t.Fatalf("premature response: %v", res.Error())
				case <-env.ctx.Done():
					t.Fatal("timed out on send")
				}
			}

			for i := 0; i < n; i++ {
				select {
				case res := <-resChan:
					assert.NoError(t, res.Error())
				case <-env.ctx.Done():
					t.Fatal("timed out on response")
				}
			}

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

func integrationTestReceiveBatchCount(n int) testDefinition {
	return namedTest(
		"can send messages with an input batch count",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			env.configVars.inputBatchCount = n

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}

			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
			}

			var tran types.Transaction
			select {
			case tran = <-input.TransactionChan():
			case <-env.ctx.Done():
				t.Fatal("timed out on receive")
			}

			assert.Equal(t, n, tran.Payload.Len())
			tran.Payload.Iter(func(_ int, p types.Part) error {
				messageInSet(t, true, false, p, set)
				return nil
			})

			select {
			case tran.ResponseChan <- response.NewAck():
			case <-env.ctx.Done():
				t.Fatal("timed out on response")
			}
		},
	)
}

func integrationTestStreamSequential(n int) testDefinition {
	return namedTest(
		"can send and receive data sequentially",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}

			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
			}

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

// The input is created after the output has written data.
func integrationTestStreamIsolated(n int) testDefinition {
	return namedTest(
		"can send and receive data isolated",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			set := map[string][]string{}

			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
			}

			input := initInput(t, env)
			t.Cleanup(func() {
				closeConnectors(t, input, nil)
			})

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

func integrationTestStreamParallel(n int) testDefinition {
	return namedTest(
		"can send and receive data in parallel",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction, n)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
			}

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					payload := fmt.Sprintf("hello world: %v", i)
					require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
				}
			}()

			go func() {
				defer wg.Done()
				for len(set) > 0 {
					messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
				}
			}()

			wg.Wait()
		},
	)
}

func integrationTestStreamParallelLossy(n int) testDefinition {
	return namedTest(
		"can send and receive data in parallel lossy",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
			}

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					payload := fmt.Sprintf("hello world: %v", i)
					require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
				}
			}()

			go func() {
				defer wg.Done()
				rejected := 0
				for i := 0; i < n; i++ {
					if i%10 == 1 {
						rejected++
						messageInSet(
							t, false, true,
							receiveMessage(env.ctx, t, input.TransactionChan(), errors.New("rejected just cus")),
							set,
						)
					} else {
						messageInSet(t, true, true, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
					}
				}

				t.Log("Finished first loop, looping through rejected messages.")
				for len(set) > 0 {
					messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
				}
			}()

			wg.Wait()
		},
	)
}

func integrationTestStreamParallelLossyThroughReconnect(n int) testDefinition {
	return namedTest(
		"can send and receive data in parallel lossy through reconnect",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
			}

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					payload := fmt.Sprintf("hello world: %v", i)
					require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
				}
			}()

			go func() {
				defer wg.Done()
				rejected := 0
				for i := 0; i < n; i++ {
					if i%10 == 1 {
						rejected++
						messageInSet(
							t, false, env.allowDuplicateMessages,
							receiveMessage(env.ctx, t, input.TransactionChan(), errors.New("rejected just cus")),
							set,
						)
					} else {
						messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
					}
				}

				closeConnectors(t, input, nil)

				input = initInput(t, env)
				defer closeConnectors(t, input, nil)

				t.Log("Finished first loop, looping through rejected messages.")
				for len(set) > 0 {
					messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
				}
			}()

			wg.Wait()
		},
	)
}
