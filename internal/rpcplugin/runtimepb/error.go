// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runtimepb

import (
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ProtoToError converts a protobuf error to a Go error.
func ProtoToError(err *Error) error {
	if err == nil {
		return nil
	}
	msg := err.GetMessage()
	switch detail := err.GetDetail().(type) {
	case *Error_Backoff:
		return service.NewErrBackOff(errors.New(msg), detail.Backoff.AsDuration())
	case *Error_NotConnected_:
		return service.ErrNotConnected
	case *Error_EndOfInput_:
		return service.ErrEndOfInput
	}
	if msg == "" {
		return nil
	}
	return errors.New(msg)
}

// ErrorToProto converts a Go error to a protobuf error.
func ErrorToProto(err error) *Error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if msg == "" {
		msg = "unknown error"
	}
	if errors.Is(err, service.ErrNotConnected) {
		return &Error{
			Message: msg,
			Detail:  &Error_NotConnected_{NotConnected: &Error_NotConnected{}},
		}
	}
	if errors.Is(err, service.ErrEndOfInput) {
		return &Error{
			Message: msg,
			Detail:  &Error_EndOfInput_{EndOfInput: &Error_EndOfInput{}},
		}
	}
	var backoffErr *service.ErrBackOff
	if errors.As(err, &backoffErr) {
		return &Error{
			Message: backoffErr.Error(),
			Detail:  &Error_Backoff{Backoff: durationpb.New(backoffErr.Wait)},
		}
	}
	return &Error{Message: msg}
}
