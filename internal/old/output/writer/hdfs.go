package writer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/colinmarc/hdfs"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

//------------------------------------------------------------------------------

// HDFSConfig contains configuration fields for the HDFS output type.
type HDFSConfig struct {
	Hosts       []string      `json:"hosts" yaml:"hosts"`
	User        string        `json:"user" yaml:"user"`
	Directory   string        `json:"directory" yaml:"directory"`
	Path        string        `json:"path" yaml:"path"`
	MaxInFlight int           `json:"max_in_flight" yaml:"max_in_flight"`
	Batching    policy.Config `json:"batching" yaml:"batching"`
}

// NewHDFSConfig creates a new Config with default values.
func NewHDFSConfig() HDFSConfig {
	return HDFSConfig{
		Hosts:       []string{},
		User:        "",
		Directory:   "",
		Path:        `${!count("files")}-${!timestamp_unix_nano()}.txt`,
		MaxInFlight: 64,
		Batching:    policy.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// HDFS is a benthos writer.Type implementation that writes messages to a
// HDFS directory.
type HDFS struct {
	conf HDFSConfig

	directory *field.Expression

	path *field.Expression

	client *hdfs.Client

	log   log.Modular
	stats metrics.Type
}

// NewHDFSV2 creates a new HDFS writer.Type.
func NewHDFSV2(
	conf HDFSConfig,
	mgr interop.Manager,
	log log.Modular,
	stats metrics.Type,
) (*HDFS, error) {
	path, err := mgr.BloblEnvironment().NewField(conf.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	directory, err := mgr.BloblEnvironment().NewField(conf.Directory)
	if err != nil {
		return nil, fmt.Errorf("failed to parse directory expression: %v", err)
	}
	return &HDFS{
		conf:      conf,
		directory: directory,
		path:      path,
		log:       log,
		stats:     stats,
	}, nil
}

// ConnectWithContext attempts to establish a connection to the target HDFS
// host.
func (h *HDFS) ConnectWithContext(ctx context.Context) error {
	return h.Connect()
}

// Connect attempts to establish a connection to the target HDFS host.
func (h *HDFS) Connect() error {
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

	h.log.Infof("Writing message parts as files to HDFS directory: %v\n", h.conf.Directory)
	return nil
}

// WriteWithContext attempts to write message contents to a target HDFS
// directory as files.
func (h *HDFS) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	return h.Write(msg)
}

// Write attempts to write message contents to a target HDFS directory as files.
func (h *HDFS) Write(msg *message.Batch) error {
	if h.client == nil {
		return component.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p *message.Part) error {
		path := h.path.String(i, msg)
		directory := h.directory.String(i, msg)
		filePath := filepath.Join(directory, path)

		err := h.client.MkdirAll(directory, os.ModeDir|0o644)
		if err != nil {
			return err
		}

		fw, err := h.client.Create(filePath)
		if err != nil {
			return err
		}

		if _, err := fw.Write(p.Get()); err != nil {
			return err
		}
		fw.Close()
		return nil
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (h *HDFS) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (h *HDFS) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
