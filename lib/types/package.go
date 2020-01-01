// Package types defines any general structs and interfaces used throughout the
// benthos code base.
//
// Benthos uses abstract types to represent arbitrary producers and consumers of
// data to its core components. This allows us to construct types for piping
// data in various arrangements without regard for the specific destinations and
// sources of our data.
//
// The basic principle behind a producer/consumer relationship is that a
// producer pipes data to the consumer in lock-step, where for each message sent
// it will expect a response that confirms the message was received and
// propagated onwards.
//
// Messages and responses are sent via channels, and in order to instigate this
// pairing each type is expected to create and maintain ownership of its
// respective sending channel.
package types
