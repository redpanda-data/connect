package reader

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// FilesConfig contains configuration for the Files input type.
type FilesConfig struct {
	Path        string `json:"path" yaml:"path"`
	DeleteFiles bool   `json:"delete_files" yaml:"delete_files"`
}

// NewFilesConfig creates a new FilesConfig with default values.
func NewFilesConfig() FilesConfig {
	return FilesConfig{
		Path:        "",
		DeleteFiles: false,
	}
}

//------------------------------------------------------------------------------

// Files is an input type that reads file contents at a path as messages.
type Files struct {
	targets []string
	delete  bool
}

// NewFiles creates a new Files input type.
func NewFiles(conf FilesConfig) (*Files, error) {
	f := Files{
		delete: conf.DeleteFiles,
	}

	if info, err := os.Stat(conf.Path); err != nil {
		return nil, err
	} else if !info.IsDir() {
		f.targets = append(f.targets, conf.Path)
		return &f, nil
	}

	err := filepath.Walk(conf.Path, func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if info.IsDir() {
			return nil
		}
		f.targets = append(f.targets, path)
		return nil
	})

	return &f, err
}

// ConnectWithContext establishes a connection.
func (f *Files) ConnectWithContext(ctx context.Context) (err error) {
	return nil
}

// ReadWithContext a new Files message.
func (f *Files) ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error) {
	if len(f.targets) == 0 {
		return nil, nil, component.ErrTypeClosed
	}

	path := f.targets[0]
	f.targets = f.targets[1:]

	file, openerr := os.Open(path)
	if openerr != nil {
		return nil, nil, fmt.Errorf("failed to read file '%v': %v", path, openerr)
	}
	defer file.Close()

	msgBytes, readerr := io.ReadAll(file)
	if readerr != nil {
		return nil, nil, readerr
	}

	msg := message.QuickBatch([][]byte{msgBytes})
	msg.Get(0).MetaSet("path", path)
	return msg, func(ctx context.Context, res types.Response) error {
		if f.delete {
			if res.Error() == nil {
				return os.Remove(path)
			}
		}
		return nil
	}, nil
}

// CloseAsync shuts down the Files input and stops processing requests.
func (f *Files) CloseAsync() {
}

// WaitForClose blocks until the Files input has closed down.
func (f *Files) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
