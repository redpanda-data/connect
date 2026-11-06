// Copyright 2026 Redpanda Data, Inc.
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

// Package writecodec defines the codecs that file-writing outputs use to write
// message bytes to a file.
package writecodec

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Field returns the codec field of an output that writes messages to files.
func Field(name string) *service.ConfigField {
	return service.NewStringAnnotatedEnumField(name, map[string]string{
		"all-bytes": "Only applicable to file based outputs. Writes each message to a file in full, if the file already exists the old content is deleted.",
		"append":    "Append each message to the output stream without any delimiter or special encoding.",
		"lines":     "Append each message to the output stream followed by a line break.",
		"delim:x":   "Append each message to the output stream followed by a custom delimiter.",
	}).
		Description("The way in which the bytes of messages are written to the file. With the `delim:x` codec, each message is followed by the custom delimiter `x`, which can be any character sequence. A delimiter is not added to a message that already ends with it.").
		ShortDescription("How the bytes of messages are written into the output data stream.").
		LintRule("").
		Examples("lines", "delim:\t", "delim:foobar")
}
