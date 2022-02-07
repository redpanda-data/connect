package integration

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StreamTestOpenClose ensures that both the input and output can be started and
// stopped within a reasonable length of time. A single message is sent to check
// the connection.
func StreamTestOpenClose() StreamTestDefinition {
	return namedStreamTest(
		"can open and close",
		func(t *testing.T, env *streamTestEnvironment) {
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

// StreamTestOpenCloseIsolated ensures that both the input and output can be
// started and stopped within a reasonable length of time. A single message is
// sent to check the connection but the input is only started after the message
// is sent.
func StreamTestOpenCloseIsolated() StreamTestDefinition {
	return namedStreamTest(
		"can open and close isolated",
		func(t *testing.T, env *streamTestEnvironment) {
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

// StreamTestMetadata ensures that we are able to send and receive metadata
// values.
func StreamTestMetadata() StreamTestDefinition {
	return namedStreamTest(
		"can send and receive metadata",
		func(t *testing.T, env *streamTestEnvironment) {
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

// StreamTestMetadataFilter ensures that we are able to send and receive
// metadata values, and that they are filtered. The provided config template
// should inject the variable $OUTPUT_META_EXCLUDE_PREFIX into the output
// metadata filter field.
func StreamTestMetadataFilter() StreamTestDefinition {
	return namedStreamTest(
		"can send and receive metadata filtered",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			env.configVars.OutputMetaExcludePrefix = "f"

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

			p := receiveMessage(env.ctx, t, input.TransactionChan(), nil)
			assert.Empty(t, p.MetaGet("foo"))
			messageMatch(t, p, "hello world", "bar", "bar_value")
		},
	)
}

// StreamTestSendBatch ensures we can send a batch of a given size.
func StreamTestSendBatch(n int) StreamTestDefinition {
	return namedStreamTest(
		"can send a message batch",
		func(t *testing.T, env *streamTestEnvironment) {
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
			err := sendBatch(env.ctx, t, tranChan, payloads)
			assert.NoError(t, err)

			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

// StreamTestSendBatches ensures that we can send N batches of M parallelism.
func StreamTestSendBatches(batchSize, batches, parallelism int) StreamTestDefinition {
	return namedStreamTest(
		"can send many message batches",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			require.Greater(t, parallelism, 0)

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}
			for j := 0; j < batches; j++ {
				for i := 0; i < batchSize; i++ {
					payload := fmt.Sprintf("hello world %v", j*batches+i)
					set[payload] = nil
				}
			}

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
						assert.NoError(t, sendBatch(env.ctx, t, tranChan, batch))
					}
				}()
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				for len(set) > 0 {
					messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
				}
			}()

			for j := 0; j < batches; j++ {
				payloads := []string{}
				for i := 0; i < batchSize; i++ {
					payload := fmt.Sprintf("hello world %v", j*batches+i)
					payloads = append(payloads, payload)
				}
				batchChan <- payloads
			}
			close(batchChan)

			wg.Wait()
		},
	)
}

// StreamTestSendBatchCount ensures we can send batches using a configured batch
// count.
func StreamTestSendBatchCount(n int) StreamTestDefinition {
	return namedStreamTest(
		"can send messages with an output batch count",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			env.configVars.OutputBatchCount = n

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
				msg := message.QuickBatch(nil)
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

// StreamTestSendBatchCountIsolated checks batches can be sent and then
// received. The input is created after the output has written data.
func StreamTestSendBatchCountIsolated(n int) StreamTestDefinition {
	return namedStreamTest(
		"can send messages with an output batch count isolated",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			env.configVars.OutputBatchCount = n

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			resChan := make(chan types.Response)

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world %v", i)
				set[payload] = nil
				msg := message.QuickBatch(nil)
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

// StreamTestReceiveBatchCount tests that batches can be consumed with an input
// configured batch count.
func StreamTestReceiveBatchCount(n int) StreamTestDefinition {
	return namedStreamTest(
		"can send messages with an input batch count",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			env.configVars.InputBatchCount = n

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
			_ = tran.Payload.Iter(func(_ int, p *message.Part) error {
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

// StreamTestStreamSequential tests that data can be sent and received, where
// data is sent sequentially.
func StreamTestStreamSequential(n int) StreamTestDefinition {
	return namedStreamTest(
		"can send and receive data sequentially",
		func(t *testing.T, env *streamTestEnvironment) {
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

// StreamTestStreamIsolated tests that data can be sent and received, where data
// is sent sequentially. The input is created after the output has written data.
func StreamTestStreamIsolated(n int) StreamTestDefinition {
	return namedStreamTest(
		"can send and receive data isolated",
		func(t *testing.T, env *streamTestEnvironment) {
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

// StreamTestCheckpointCapture ensures that data received out of order doesn't
// result in wrongly acknowledged messages.
func StreamTestCheckpointCapture() StreamTestDefinition {
	return namedStreamTest(
		"respects checkpointed offsets",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			go func() {
				require.NoError(t, sendMessage(env.ctx, t, tranChan, "A"))
				require.NoError(t, sendMessage(env.ctx, t, tranChan, "B"))
				require.NoError(t, sendMessage(env.ctx, t, tranChan, "C"))
				require.NoError(t, sendMessage(env.ctx, t, tranChan, "D"))
				require.NoError(t, sendMessage(env.ctx, t, tranChan, "E"))
			}()

			var msg *message.Part
			responseChans := make([]chan<- types.Response, 5)

			msg, responseChans[0] = receiveMessageNoRes(env.ctx, t, input.TransactionChan())
			assert.Equal(t, "A", string(msg.Get()))
			sendResponse(env.ctx, t, responseChans[0], nil)

			msg, responseChans[1] = receiveMessageNoRes(env.ctx, t, input.TransactionChan())
			assert.Equal(t, "B", string(msg.Get()))
			sendResponse(env.ctx, t, responseChans[1], nil)

			msg, responseChans[2] = receiveMessageNoRes(env.ctx, t, input.TransactionChan())
			assert.Equal(t, "C", string(msg.Get()))

			msg, responseChans[3] = receiveMessageNoRes(env.ctx, t, input.TransactionChan())
			assert.Equal(t, "D", string(msg.Get()))
			sendResponse(env.ctx, t, responseChans[3], nil)

			msg, responseChans[4] = receiveMessageNoRes(env.ctx, t, input.TransactionChan())
			assert.Equal(t, "E", string(msg.Get()))

			sendResponse(env.ctx, t, responseChans[2], errors.New("rejecting just cus"))
			sendResponse(env.ctx, t, responseChans[4], errors.New("rejecting just cus"))

			closeConnectors(t, input, nil)

			select {
			case <-time.After(time.Second * 5):
			case <-env.ctx.Done():
				t.Fatal(env.ctx.Err())
			}

			input = initInput(t, env)
			t.Cleanup(func() {
				closeConnectors(t, input, nil)
			})

			msg = receiveMessage(env.ctx, t, input.TransactionChan(), nil)
			assert.Equal(t, "C", string(msg.Get()))

			msg = receiveMessage(env.ctx, t, input.TransactionChan(), nil)
			assert.Equal(t, "D", string(msg.Get()))

			msg = receiveMessage(env.ctx, t, input.TransactionChan(), nil)
			assert.Equal(t, "E", string(msg.Get()))
		},
	)
}

// StreamTestStreamParallel tests data transfer with parallel senders.
func StreamTestStreamParallel(n int) StreamTestDefinition {
	return namedStreamTest(
		"can send and receive data in parallel",
		func(t *testing.T, env *streamTestEnvironment) {
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

// StreamTestStreamSaturatedUnacked writes N messages as a backlog, then
// consumes half of those messages without acking them, and then after a pause
// acknowledges them all and resumes consuming.
//
// The purpose of this test is to ensure that after a period of back pressure is
// applied the input correctly resumes.
func StreamTestStreamSaturatedUnacked(n int) StreamTestDefinition {
	return namedStreamTest(
		"can consume data without acking and resume",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction, n)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, input, output)
			})

			set := map[string][]string{}
			for i := 0; i < n*2; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
			}

			resChans := make([]chan<- types.Response, n/2)
			for i := range resChans {
				var b *message.Part
				b, resChans[i] = receiveMessageNoRes(env.ctx, t, input.TransactionChan())
				messageInSet(t, true, env.allowDuplicateMessages, b, set)
			}

			<-time.After(time.Second * 5)
			for _, rChan := range resChans {
				sendResponse(env.ctx, t, rChan, nil)
			}

			// Consume all remaining messages
			for len(set) > 0 {
				messageInSet(t, true, env.allowDuplicateMessages, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
			}
		},
	)
}

// StreamTestAtLeastOnceDelivery ensures data is delivered through nacks and
// restarts.
func StreamTestAtLeastOnceDelivery() StreamTestDefinition {
	return namedStreamTest(
		"at least once delivery",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			expectedMessages := map[string]struct{}{
				"A": {}, "B": {}, "C": {}, "D": {}, "E": {},
			}
			go func() {
				for k := range expectedMessages {
					require.NoError(t, sendMessage(env.ctx, t, tranChan, k))
				}
			}()

			var msg *message.Part
			badResponseChans := []chan<- types.Response{}

			for i := 0; i < len(expectedMessages); i++ {
				msg, responseChan := receiveMessageNoRes(env.ctx, t, input.TransactionChan())
				key := string(msg.Get())
				assert.Contains(t, expectedMessages, key)
				delete(expectedMessages, key)
				if key != "C" && key != "E" {
					sendResponse(env.ctx, t, responseChan, nil)
				} else {
					badResponseChans = append(badResponseChans, responseChan)
				}
			}

			for _, rChan := range badResponseChans {
				sendResponse(env.ctx, t, rChan, errors.New("rejecting just cus"))
			}

			select {
			case <-time.After(time.Second * 5):
			case <-env.ctx.Done():
				t.Fatal(env.ctx.Err())
			}

			closeConnectors(t, input, nil)

			select {
			case <-time.After(time.Second * 5):
			case <-env.ctx.Done():
				t.Fatal(env.ctx.Err())
			}

			input = initInput(t, env)
			t.Cleanup(func() {
				closeConnectors(t, input, nil)
			})

			expectedMessages = map[string]struct{}{
				"C": {}, "E": {},
			}

			for i := 0; i < len(expectedMessages); i++ {
				msg = receiveMessage(env.ctx, t, input.TransactionChan(), nil)
				key := string(msg.Get())
				assert.Contains(t, expectedMessages, key)
				delete(expectedMessages, key)
			}
		},
	)
}

// StreamTestStreamParallelLossy ensures that data is delivered through parallel
// nacks.
func StreamTestStreamParallelLossy(n int) StreamTestDefinition {
	return namedStreamTest(
		"can send and receive data in parallel lossy",
		func(t *testing.T, env *streamTestEnvironment) {
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

// StreamTestStreamParallelLossyThroughReconnect ensures data is delivered
// through nacks and restarts.
func StreamTestStreamParallelLossyThroughReconnect(n int) StreamTestDefinition {
	return namedStreamTest(
		"can send and receive data in parallel lossy through reconnect",
		func(t *testing.T, env *streamTestEnvironment) {
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
				t.Cleanup(func() {
					closeConnectors(t, input, nil)
				})

				t.Log("Finished first loop, looping through rejected messages.")
				for len(set) > 0 {
					messageInSet(t, true, true, receiveMessage(env.ctx, t, input.TransactionChan(), nil), set)
				}
			}()

			wg.Wait()
		},
	)
}

// GetMessageFunc is a closure used to extract message contents from an output
// directly and can be used to test outputs without the need for an input in the
// config template.
type GetMessageFunc func(ctx context.Context, testID, messageID string) (string, []string, error)

// StreamTestOutputOnlySendSequential tests a config template without an input.
func StreamTestOutputOnlySendSequential(n int, getFn GetMessageFunc) StreamTestDefinition {
	return namedStreamTest(
		"can send to output",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			set := map[string]string{}
			for i := 0; i < n; i++ {
				id := strconv.Itoa(i)
				payload := fmt.Sprintf(`{"content":"hello world","id":%v}`, id)
				set[id] = payload
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload, "id", id))
			}

			for k, exp := range set {
				act, _, err := getFn(env.ctx, env.configVars.ID, k)
				require.NoError(t, err)
				assert.Equal(t, exp, act)
			}
		},
	)
}

// StreamTestOutputOnlySendBatch tests a config template without an input.
func StreamTestOutputOnlySendBatch(n int, getFn GetMessageFunc) StreamTestDefinition {
	return namedStreamTest(
		"can send to output as batch",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			set := map[string]string{}
			batch := []string{}
			for i := 0; i < n; i++ {
				id := strconv.Itoa(i)
				payload := fmt.Sprintf(`{"content":"hello world","id":%v}`, id)
				set[id] = payload
				batch = append(batch, payload)
			}
			require.NoError(t, sendBatch(env.ctx, t, tranChan, batch))

			for k, exp := range set {
				act, _, err := getFn(env.ctx, env.configVars.ID, k)
				require.NoError(t, err)
				assert.Equal(t, exp, act)
			}
		},
	)
}

// StreamTestOutputOnlyOverride tests a config template without an input where
// duplicate IDs are sent (where we expect updates).
func StreamTestOutputOnlyOverride(getFn GetMessageFunc) StreamTestDefinition {
	return namedStreamTest(
		"can send to output and override value",
		func(t *testing.T, env *streamTestEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})

			first := `{"content":"this should be overridden","id":1}`
			exp := `{"content":"hello world","id":1}`
			require.NoError(t, sendMessage(env.ctx, t, tranChan, first))
			require.NoError(t, sendMessage(env.ctx, t, tranChan, exp))

			act, _, err := getFn(env.ctx, env.configVars.ID, "1")
			require.NoError(t, err)
			assert.Equal(t, exp, act)
		},
	)
}
