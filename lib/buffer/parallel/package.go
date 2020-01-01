// Package parallel contains implementations of various buffer types where the
// buffer can be consumed by any number of parallel consumer threads. Therefore,
// since it is possible for consumers to requeue a message if the propagation
// failed, it is possible for messages to be consumed out of sequence.
package parallel
