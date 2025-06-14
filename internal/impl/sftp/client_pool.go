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
	"golang.org/x/crypto/ssh"
)

func newClientPool(
	newSshConn func() (*ssh.Client, error),
	newClient func(*ssh.Client) (*sftp.Client, error),
) (*clientPool, error) {
	cp := clientPool{
		newSshConn: newSshConn,
		newClient:  newClient,
		sshConn:    nil,
		client:     nil,
	}
	// we instantiate our pool without a live connection/client, so that we
	// have an early test of our preparation/repair function:
	if err := cp.prepareClient(); err != nil {
		return nil, err
	}
	return &cp, nil
}

type clientPool struct {
	newSshConn func() (*ssh.Client, error)
	newClient  func(*ssh.Client) (*sftp.Client, error)

	lock    sync.Mutex
	sshConn *ssh.Client
	client  *sftp.Client
	closed  bool
}

// prepareClient creates a new SSH connection and SFTP client if either/both
// are missing.
func (c *clientPool) prepareClient() error {
	var err error

	if c.client != nil {
		return nil
	}

	// only create a new SSH connection if we don't have one already:
	if c.sshConn == nil {
		c.sshConn, err = c.newSshConn()
		if err != nil {
			return err
		}
	}

	c.client, err = c.newClient(c.sshConn)
	return err
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

func (c *clientPool) Close() (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.client != nil {
		err = c.client.Close()
		c.client = nil
	}
	if c.sshConn != nil {
		err = errors.Join(err, c.sshConn.Close())
		c.sshConn = nil
	}
	c.closed = true
	return
}

func clientPoolDo(c *clientPool, fn func(*sftp.Client) error) error {
	_, err := clientPoolDoReturning(c, func(client *sftp.Client) (struct{}, error) {
		err := fn(client)
		return struct{}{}, err
	})
	return err
}

// clientPoolDoReturning executes a function with our pool SFTP client and
// returns the resulting value/error.
//
// In the case the clientPool is used from an AckFn after the input is
// closed, we create temporary connection/client to fulfil the operation,
// then immediately close them again afterwards.
func clientPoolDoReturning[T any](c *clientPool, fn func(*sftp.Client) (T, error)) (T, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var (
		zero         T
		err          error
		afterClosure = c.closed // so we can clean up after ourselves
	)

	if err = c.prepareClient(); err != nil {
		return zero, err
	}

	result, err := fn(c.client)
	if afterClosure || errors.Is(err, sftp.ErrSSHFxConnectionLost) {
		_ = c.client.Close()
		c.client = nil
		_ = c.sshConn.Close()
		c.sshConn = nil
	}
	return result, err
}
