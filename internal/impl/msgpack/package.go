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

package msgpack

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	msgpack.Register(json.Number("0"),
		func(enc *msgpack.Encoder, value reflect.Value) error {
			strValue := value.String()
			if intValue, err := strconv.ParseInt(strValue, 10, 64); err == nil {
				if err := enc.EncodeInt(intValue); err != nil {
					return err
				}
			} else if uintValue, err := strconv.ParseUint(strValue, 10, 64); err == nil {
				if err := enc.EncodeUint(uintValue); err != nil {
					return err
				}
			} else if floatValue, err := strconv.ParseFloat(strValue, 64); err == nil {
				if err := enc.EncodeFloat64(floatValue); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("unable to parse %s neither as int nor as float", strValue)
			}
			return nil
		},
		func(dec *msgpack.Decoder, value reflect.Value) error {
			return nil
		},
	)
}
