package writer

import (
	"context"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/colinmarc/hdfs"
)

//------------------------------------------------------------------------------

// HDFSConfig contains configuration fields for the HDFS output type.
type HDFSConfig struct {
	Hosts       []string `json:"hosts" yaml:"hosts"`
	User        string   `json:"user" yaml:"user"`
	Directory   string   `json:"directory" yaml:"directory"`
	Path        string   `json:"path" yaml:"path"`
	MaxInFlight int      `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewHDFSConfig creates a new Config with default values.
func NewHDFSConfig() HDFSConfig {
	return HDFSConfig{
		Hosts:       []string{"localhost:9000"},
		User:        "benthos_hdfs",
		Directory:   "",
		Path:        "${!count:files}-${!timestamp_unix_nano}.txt",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// HDFS is a benthos writer.Type implementation that writes messages to a
// HDFS directory.
type HDFS struct {
	conf HDFSConfig

	pathBytes       []byte
	interpolatePath bool

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
	pathBytes := []byte(conf.Path)
	interpolatePath := text.ContainsFunctionVariables(pathBytes)
	return &HDFS{
		conf:            conf,
		pathBytes:       pathBytes,
		interpolatePath: interpolatePath,
		log:             log,
		stats:           stats,
	}
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
func (h *HDFS) WriteWithContext(ctx context.Context, msg types.Message) error {
	return h.Write(msg)
}

// Write attempts to write message contents to a target HDFS directory as files.
func (h *HDFS) Write(msg types.Message) error {
	if h.client == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		path := h.conf.Path
		if h.interpolatePath {
			path = string(text.ReplaceFunctionVariables(message.Lock(msg, i), h.pathBytes))
		}
		filePath := filepath.Join(h.conf.Directory, path)

		err := h.client.MkdirAll(h.conf.Directory, 0644)
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
