/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package streaming

import (
	"errors"
	"fmt"
)

// APIError is an API response when the streaming API has an error.
type APIError struct {
	StatusCode int    `json:"status_code"`
	Message    string `json:"message"`
}

var _ error = APIError{}

// Error statisfies the Error interface
func (e APIError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = "(no message)"
	}
	return fmt.Sprintf("API error (status_code=%d): %s", e.StatusCode, msg)
}

// IsTableNotExistsError returns true if the table does not exist (or the user is not authorized to see it).
func IsTableNotExistsError(err error) bool {
	var restErr APIError
	return errors.As(err, &restErr) && restErr.StatusCode == responseTableNotExist
}
