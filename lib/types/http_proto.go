package types

// HTTPResponse is a struct expected as an HTTP response after sending a
// message. The intention is to confirm that the message has been received
// successfully.
type HTTPResponse struct {
	Error string `json:"error"`
}

// HTTPMessage is a struct containing a benthos message, to be sent over the
// wire in an HTTP request. Message parts are sent as JSON strings.
type HTTPMessage struct {
	Parts []string `json:"parts"`
}
