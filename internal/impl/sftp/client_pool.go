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

package sftp

import (
	"errors"
	"io/fs"
	"sync"

	"github.com/pkg/sftp"
)

func newClientPool(newClient func() (*sftp.Client, error)) (*clientPool, error) {
	client, err := newClient()
	if err != nil {
		return nil, err
	}
	return &clientPool{
		newClient: newClient,
		client:    client,
	}, nil
}

type clientPool struct {
	newClient func() (*sftp.Client, error)

	lock   sync.Mutex
	client *sftp.Client
	closed bool
}

func (c *clientPool) Open(path string) (*sftp.File, error) {
	return clientPoolDoReturning(c, func(client *sftp.Client) (*sftp.File, error) {
		return client.Open(path)
	})
}

func (c *clientPool) Glob(path string) ([]string, error) {
	return clientPoolDoReturning(c, func(client *sftp.Client) ([]string, error) {
		return client.Glob(path)
	})
}

func (c *clientPool) Stat(path string) (fs.FileInfo, error) {
	return clientPoolDoReturning(c, func(client *sftp.Client) (fs.FileInfo, error) {
		return client.Stat(path)
	})
}

func (c *clientPool) Remove(path string) error {
	return clientPoolDo(c, func(client *sftp.Client) error {
		return client.Remove(path)
	})
}

func (c *clientPool) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.client != nil {
		err := c.client.Close()
		c.client = nil
		return err
	}
	return nil
}

func clientPoolDo(c *clientPool, fn func(*sftp.Client) error) error {
	_, err := clientPoolDoReturning(c, func(client *sftp.Client) (struct{}, error) {
		err := fn(client)
		return struct{}{}, err
	})
	return err
}

func clientPoolDoReturning[T any](c *clientPool, fn func(*sftp.Client) (T, error)) (T, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var zero T

	// In the case that the clientPool is used from an AckFn after the input is
	// closed, we create temporary client to fulfil the operation, then
	// immediately close it.
	if c.closed {
		client, err := c.newClient()
		if err != nil {
			return zero, err
		}
		result, err := fn(client)
		_ = client.Close()
		return result, err
	}

	if c.client == nil {
		client, err := c.newClient()
		if err != nil {
			return zero, err
		}
		c.client = client
	}

	result, err := fn(c.client)
	if errors.Is(err, sftp.ErrSSHFxConnectionLost) {
		_ = c.client.Close()
		c.client = nil
	}
	return result, err
}
