// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package timeplus

import "context"

// Writer is the interface. Currently only http writer is implemented. Caller needs to make sure all writes contain the same `cols`
type Writer interface {
	Write(ctx context.Context, cols []string, rows [][]any) error
}

// Reader is the interface. Called MUST guarantee that the `Run` method is called before `Read` or `Close`
type Reader interface {
	Run(sql string) error
	Read(ctx context.Context) (map[string]any, error)
	Close(ctx context.Context) error
}
