// Package output defines all sinks for sending Benthos messages to a variety of
// third party destinations. All output types must implement interface
// output.Type.
//
// If the sink of an output provides a form of acknowledgement of message
// receipt then the output is responsible for propagating that acknowledgement
// back to the source of the data by sending it over the transaction response
// channel. Otherwise a standard acknowledgement is sent.
package output
