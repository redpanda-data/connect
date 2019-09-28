// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package reader

import (
	"context"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/colinmarc/hdfs"
)

//------------------------------------------------------------------------------

// HDFSConfig contains configuration fields for the HDFS input type.
type HDFSConfig struct {
	Hosts     []string `json:"hosts" yaml:"hosts"`
	User      string   `json:"user" yaml:"user"`
	Directory string   `json:"directory" yaml:"directory"`
}

// NewHDFSConfig creates a new Config with default values.
func NewHDFSConfig() HDFSConfig {
	return HDFSConfig{
		Hosts:     []string{"localhost:9000"},
		User:      "benthos_hdfs",
		Directory: "",
	}
}

//------------------------------------------------------------------------------

// HDFS is a benthos reader.Type implementation that reads messages from a
// HDFS directory.
type HDFS struct {
	conf HDFSConfig

	targets []string

	client *hdfs.Client

	log   log.Modular
	stats metrics.Type
}

// NewHDFS creates a new HDFS writer.Type.
func NewHDFS(
	conf HDFSConfig,
	log log.Modular,
	stats metrics.Type,
) *HDFS {
	return &HDFS{
		conf:  conf,
		log:   log,
		stats: stats,
	}
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to the target HDFS host.
func (h *HDFS) Connect() error {
	return h.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a connection to the target HDFS
// host.
func (h *HDFS) ConnectWithContext(ctx context.Context) error {
	if h.client != nil {
		return nil
	}

	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: h.conf.Hosts,
		User:      h.conf.User,
	})
	if err != nil {
		return err
	}

	h.client = client
	targets, err := client.ReadDir(h.conf.Directory)
	if err != nil {
		return err
	}

	for _, info := range targets {
		if !info.IsDir() {
			h.targets = append(h.targets, info.Name())
		}
	}

	h.log.Infof("Receiving files from HDFS directory: %v\n", h.conf.Directory)
	return nil
}

//------------------------------------------------------------------------------

// ReadWithContext reads a new HDFS message.
func (h *HDFS) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	msg, err := h.Read()
	if err != nil {
		return nil, nil, err
	}
	return msg, noopAsyncAckFn, nil
}

// Read a new HDFS message.
func (h *HDFS) Read() (types.Message, error) {
	if len(h.targets) == 0 {
		return nil, types.ErrTypeClosed
	}

	fileName := h.targets[0]
	h.targets = h.targets[1:]

	filePath := filepath.Join(h.conf.Directory, fileName)
	msgBytes, readerr := h.client.ReadFile(filePath)
	if readerr != nil {
		return nil, readerr
	}

	msg := message.New([][]byte{msgBytes})
	msg.Get(0).Metadata().Set("hdfs_name", fileName)
	msg.Get(0).Metadata().Set("hdfs_path", filePath)
	return msg, nil
}

// Acknowledge instructs whether unacknowledged messages have been successfully
// propagated.
func (h *HDFS) Acknowledge(err error) error {
	return nil
}

// CloseAsync shuts down the HDFS input and stops processing requests.
func (h *HDFS) CloseAsync() {
}

// WaitForClose blocks until the HDFS input has closed down.
func (h *HDFS) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
