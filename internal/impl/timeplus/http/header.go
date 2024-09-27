package http

import (
	"encoding/base64"
	"net/http"
)

// NewHeader creates a standard Timeplus HTTP header.
func NewHeader(apikey, username, password string) http.Header {
	header := http.Header{}

	header.Add("Content-Type", "application/json")

	if len(username)+len(password) > 0 {
		auth := username + ":" + password
		header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))
	} else if len(apikey) > 0 {
		header.Add("X-Api-Key", apikey)
	}

	return header
}
