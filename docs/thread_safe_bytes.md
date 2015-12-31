Thread Safe Byte Array
======================

Since Benthos uses memory-mapped files its core performance bottleneck outside of IO is storing messages into an array of bytes, and simultaneously reading messages from that array. Since inputs and outputs are asynchronous, we must have a structure that can efficiently wrap the byte array with locking mechanisms for protecting the data from dangerous parallel writes/reads.

This document details the process taken to finding our solution.

## Idiomatic Conditional Goroutine Blocking

As data is written and read from the structure there will be moments where either a writer or a reader will be blocked, either due to all messages being exhausted from the stack or by the cache running out of space. It is important in these moments that the reader/writer is blocked only for the exact duration that the blocking condition is true.

The true idiomatic way of satisfying these two conditions could be to have the structure spawn a single goroutine that both reads from an input channel when there is remaining space for messages, and writes out the next messages to an output channel when there is one. A simplified example might look like this:

```golang
func (s *Structure) loop() {
	var readChan <-chan Message
	var writeChan chan<- Message

	for {
		// If we have an unread message buffered
		if s.hasMessage() {
			writeChan = s.writeChan
		} else {
			writeChan = nil
		}

		// If we have space in our buffer for new messages
		if s.hasSpace() {
			readChan = s.readChan
		} else {
			readChan = nil
		}

		// Wait until a reader or writer is ready
		select {
		case writeChan <- s.nextMessage():
			s.popNextMessage()
		case msg := <-readChan:
			s.pushNextMessage(msg)
		}
	}
}
```

This code is simple, it is clear that we are disabling channels that we are not ready to read or write to, and then the select statement is where we block until the next asynchronous operation frees us. However, although idiomatic, this example falls short in that we will unblock a writer without confirming that the provided message actually _fits_ inside our buffer, we are only able to confirm beforehand that the buffer _is not full_.

If we want to fix this limitation we need to deviate from the simpler and more idiomatic example by eliminating the select statement and channels. We will see that both of our original conditions can easily and efficiently be met with Golangs `sync` package, specifically the `sync.Cond` type.

## Less Idiomatic Blocking

Using the `sync` package we will depart from channels and instead divide our structure into three methods `PushMessage(msg Message)`, `NextMessage() (Message, error)` and `ShiftMessage()`. Splitting the reading operations into Next and Shift means we can read without committing, this helps in case the message fails to get propagated.

The `PushMessage` method takes a message and puts it on the end of our buffer stack. This call will block until the operation has been completed. Blocking will last until there is enough space to write our message in the buffer and it is safe to do so.

Ensuring it is safe to write is as simple as locking a shared mutex between readers and writers. However, we also need to have a yielding block occur in the writer goroutine which lasts until the buffer has the space we need. We could write a loop that sleeps `n` time and continuously checks the buffer space, but this is a poor approach as it wastes CPU cycles and potentially wastes `n` time for each message write on an 'at capacity' buffer.

The correct way to perform this block is to use `sync.Cond.Broadcast`, which allows us to have a goroutine conditionally unblock one or more other goroutines. In this case we want the writing goroutine to block with `cond.Wait()` when it finds the buffer has insufficient space. Any reading goroutines can call `cond.Broadcast()` whenever space in the buffer is cleared from a `ShiftMessage()` call, prompting any blocked writers to re-check the space in the buffer at exactly the moment the space could have been made available.

Here is a simplified example of this idea:

```golang
func (s *Structure) ShiftMessage() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	if s.shiftLastMessage() {
		// Tell all blocked writers that the space in our buffer has changed.
		s.cond.Broadcast()
	}
}


func (s *Structure) PushMessage(msg Message) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	b := msg.ToBytes()
	for len(b) > s.remainingSpace() {
		// Waits for a reading goroutine to signal a change in buffer space.
		// This call unlocks the shared mutex within s.cond, and reacquires them before exiting,
		// therefore we are safely yielding to readers.
		s.cond.Wait()
	}

	s.pushBytes(b)
}
```

The `NextMessage() (Message, error)` method can be given similar blocking mechanisms for when there are no messages to read.

It is clear that we have deviated somewhat away from pure idiomatic Go, and although the resulting code is not overly complex it ought to be validated that the approach will not penalise us in performance. I have performed benchmarks myself using real world inputs and outputs, but I have also added a directory `examples` for code snippets and benchmarks for reviewers to try out. The above work is demonstrated in `blocking_test.go` and can be run with `go test -bench=.*Blocking`.
