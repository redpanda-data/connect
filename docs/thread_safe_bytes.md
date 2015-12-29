Thread Safe Byte Array
======================

The core of Benthos is quickly storing messages into an array of bytes, and subsequently reading messages from that array. Since inputs and outputs are asynchronous, we must have a structure that can efficiently wrap the byte array with locking mechanisms for protecting the data from dangerous parallel writes/reads.

As data is written and read from the structure there will be moments where either a writer or a reader will be blocked, either due to all messages being exhausted from the stack or by the cache running out of space. It is important in these moments that the read/writer is blocked only for the exact duration that the blocking condition is true.

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

This code is simple, it is clear that we are disabling channels that we are not ready to read or write to, and then the select statement is where we block until the next asynchronous operation frees us. However, this example uses a select statement for every loop, meaning every read and write operation will be bottlenecked by the time of the system call.

If we want to speed up this structure we need to deviate from the simpler and more idiomatic example by eliminating the select statement. We will see that both of our original conditions can easily and efficiently be met with Golangs `sync` package, specifically the `sync.Cond` type.

## Three New Methods

Using the new mechanisms we will divide our structure into three methods `PushMessage(msg Message)`, `NextMessage() (Message, error)` and `ShiftMessage()`. Splitting the reading operations into Next and Shift means we can read without committing, this helps in case the message fails to get propagated.

### PushMessage

The `PushMessage` method takes a message and puts it on the end of our buffer stack. This call will block until the operation has been completed. Blocking will last until there is enough space to write our message in the buffer and it is safe to do so.

Ensuring it is safe to write is as simple as locking a shared mutex between readers and writers. However, we also need to have a yielding block occur in the writer goroutine which lasts until the buffer has the space we need. Many lazy programmers at this stage will write a loop that sleeps `n` time and continuously checks the buffer space, this is a poor approach as it wastes CPU cycles and potentially wastes `n` time for each message write on an 'at capacity' buffer.

The correct way to perform this block is to use `sync.Cond.Broadcast`, this allows us to have a goroutine conditionally unblock one or more other goroutines. In this case we want the writing goroutine to block with `cond.Wait()` when it finds the buffer has insufficient space for writing. Any reading goroutines can call `cond.Broadcast()` whenever space in the buffer is cleared from a `ShiftMessage()` call, prompting any blocked writers to re-check the space in the buffer at exactly the moment the space could have been made available.

Here is a simplified example of this:

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

## Further Optimizations

In this breakdown we were focusing on the memory-only cache, with a maximum of one respective reader and writer. There is also a use case for a file-based cache where we could support multiple readers, in this case we could speed up our operations by having read/write specific locking mechanisms.
