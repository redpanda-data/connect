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

package gcp

import (
	"context"

	"github.com/anicoll/screamer/pkg/model"
	"github.com/stretchr/testify/mock"
)

type mockStreamReader struct {
	mock.Mock
}

var _ streamReader = &mockStreamReader{}

func (mt *mockStreamReader) Stream(ctx context.Context, channel chan<- *model.DataChangeRecord) error {
	args := mt.Called(ctx, channel)

	return args.Error(0)
}

func (mt *mockStreamReader) Close() error {
	args := mt.Called()
	return args.Error(0)
}
