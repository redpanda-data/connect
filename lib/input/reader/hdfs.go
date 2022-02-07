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
func (h *HDFS) ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error) {
	if len(h.targets) == 0 {
		return nil, nil, types.ErrTypeClosed
	}

	fileName := h.targets[0]
	h.targets = h.targets[1:]

	filePath := filepath.Join(h.conf.Directory, fileName)
	msgBytes, readerr := h.client.ReadFile(filePath)
	if readerr != nil {
		return nil, nil, readerr
	}

	msg := message.QuickBatch([][]byte{msgBytes})
	msg.Get(0).MetaSet("hdfs_name", fileName)
	msg.Get(0).MetaSet("hdfs_path", filePath)
	return msg, noopAsyncAckFn, nil
}

// CloseAsync shuts down the HDFS input and stops processing requests.
func (h *HDFS) CloseAsync() {
}

// WaitForClose blocks until the HDFS input has closed down.
func (h *HDFS) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
