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

package kafka

import (
	"errors"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	errSharedClientNameDuplicate = errors.New("a duplicate name for shared clients has been detected")
	errSharedClientNameNotFound  = errors.New("shared client not found")
)

// FranzSharedClientSet attempts to store a shared client with a given
// identifier in the provided resources pointer.
func FranzSharedClientSet(name string, client *FranzSharedClientInfo, res *service.Resources) error {
	reg := getSharedClientRegister(res)
	return reg.set(name, client)
}

// FranzSharedClientPop attempts to remove and return a shared client with a
// given identifier in the provided resources pointer.
func FranzSharedClientPop(name string, res *service.Resources) (*FranzSharedClientInfo, error) {
	reg := getSharedClientRegister(res)
	return reg.pop(name)
}

// FranzSharedClientUseFn defines a closure that receives shared client details.
type FranzSharedClientUseFn func(details *FranzSharedClientInfo) error

// FranzSharedClientUse attempts to access a shared client with a given
// identifier in the provided resources pointer.
func FranzSharedClientUse(name string, res *service.Resources, fn FranzSharedClientUseFn) error {
	reg := getSharedClientRegister(res)
	return reg.use(name, fn)
}

// FranzSharedClientInfo provides an active client and the connection details
// used to create it.
type FranzSharedClientInfo struct {
	Client      *kgo.Client
	ConnDetails *FranzConnectionDetails
}

//------------------------------------------------------------------------------

type franzSharedClientRegister struct {
	mut     sync.RWMutex
	clients map[string]*FranzSharedClientInfo
}

func (r *franzSharedClientRegister) set(name string, client *FranzSharedClientInfo) error {
	r.mut.Lock()
	defer r.mut.Unlock()

	if r.clients == nil {
		r.clients = map[string]*FranzSharedClientInfo{}
	}

	_, exists := r.clients[name]
	if exists {
		return errSharedClientNameDuplicate
	}

	r.clients[name] = client
	return nil
}

func (r *franzSharedClientRegister) pop(name string) (*FranzSharedClientInfo, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	if r.clients == nil {
		return nil, errSharedClientNameNotFound
	}

	e, exists := r.clients[name]
	if !exists {
		return nil, errSharedClientNameNotFound
	}

	delete(r.clients, name)
	return e, nil
}

func (r *franzSharedClientRegister) use(name string, fn func(*FranzSharedClientInfo) error) error {
	r.mut.RLock()
	defer r.mut.RUnlock()

	if r.clients == nil {
		return errSharedClientNameNotFound
	}

	e, exists := r.clients[name]
	if !exists {
		return errSharedClientNameNotFound
	}

	return fn(e)
}

//------------------------------------------------------------------------------

type franzSharedClientKeyType int

var franzSharedClientKey franzSharedClientKeyType

func getSharedClientRegister(res *service.Resources) *franzSharedClientRegister {
	// Note: we avoid allocating `.clients` here because it would be unused in
	// the majority of calls. The real world impact of this "optimisation"
	// hasn't been tested, and so it might be worth adding it in favour of
	// removing the `r.clients == nil` checks above.
	reg, _ := res.GetOrSetGeneric(franzSharedClientKey, &franzSharedClientRegister{})
	return reg.(*franzSharedClientRegister)
}
