// Package input defines consumers for aggregating data from a variety of
// sources. All consumer types must implement interface input.Type.
//
// If the source of an input consumer supports acknowledgements then the
// implementation of the input will wait for each message to reach a permanent
// destination before acknowledging it.
package input
