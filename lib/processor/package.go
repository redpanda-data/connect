// Package processor contains implementations of types.Processor, which perform
// an arbitrary operation on a message and either returns >0 messages to be
// propagated towards a sink, or a response to be sent back to the message
// source.
package processor
