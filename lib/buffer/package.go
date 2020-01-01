// Package buffer is both a types.Consumer and types.Producer implementation
// that is able to sit between other stream components, effectively decoupling
// their transaction channels by storing messages in a buffer implementation.
//
// Buffers are not needed within Benthos, and should not be used unless there is
// a specific problem to be solved with one.
package buffer
