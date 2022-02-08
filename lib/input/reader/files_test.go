package reader

import (
	"context"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
)

//------------------------------------------------------------------------------

func TestFilesDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	tmpInnerDir, err := os.MkdirTemp(tmpDir, "benthos_inner")
	if err != nil {
		t.Fatal(err)
	}

	var tmpFile *os.File
	if tmpFile, err = os.CreateTemp(tmpDir, "f1"); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.WriteString("foo"); err != nil {
		t.Fatal(err)
	}
	if err = tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	if tmpFile, err = os.CreateTemp(tmpInnerDir, "f2"); err != nil {
		t.Fatal(err)
	}
	if _, err = tmpFile.WriteString("bar"); err != nil {
		t.Fatal(err)
	}
	if err = tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	exp := map[string]struct{}{
		"foo": {},
		"bar": {},
	}
	act := map[string]struct{}{}

	conf := NewFilesConfig()
	conf.Path = tmpDir

	var f Async
	if f, err = NewFiles(conf); err != nil {
		t.Fatal(err)
	}

	if err = f.ConnectWithContext(context.Background()); err != nil {
		t.Error(err)
	}

	var msg *message.Batch
	if msg, _, err = f.ReadWithContext(context.Background()); err != nil {
		t.Error(err)
	} else {
		resStr := string(msg.Get(0).Get())
		if _, exists := act[resStr]; exists {
			t.Errorf("Received duplicate message: %v", resStr)
		}
		act[resStr] = struct{}{}
	}
	if msg, _, err = f.ReadWithContext(context.Background()); err != nil {
		t.Error(err)
	} else {
		resStr := string(msg.Get(0).Get())
		if _, exists := act[resStr]; exists {
			t.Errorf("Received duplicate message: %v", resStr)
		}
		act[resStr] = struct{}{}
	}
	if _, _, err = f.ReadWithContext(context.Background()); err != component.ErrTypeClosed {
		t.Error(err)
	}

	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestFilesFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "f1")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.WriteString("foo"); err != nil {
		t.Fatal(err)
	}
	if err = tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	exp := map[string]struct{}{
		"foo": {},
	}
	act := map[string]struct{}{}

	conf := NewFilesConfig()
	conf.Path = tmpFile.Name()

	var f *Files
	if f, err = NewFiles(conf); err != nil {
		t.Fatal(err)
	}

	var msg *message.Batch
	var ackFn AsyncAckFn
	if msg, ackFn, err = f.ReadWithContext(context.Background()); err != nil {
		t.Error(err)
	} else {
		resStr := string(msg.Get(0).Get())
		if _, exists := act[resStr]; exists {
			t.Errorf("Received duplicate message: %v", resStr)
		}
		act[resStr] = struct{}{}
		if err = ackFn(context.Background(), response.NewAck()); err != nil {
			t.Error(err)
		}
	}
	if _, _, err = f.ReadWithContext(context.Background()); err != component.ErrTypeClosed {
		t.Error(err)
	}

	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestFilesBadPath(t *testing.T) {
	conf := NewFilesConfig()
	conf.Path = "fdgdfkte34%#@$%#$%KL@#K$@:L#$23k;32l;23"

	if _, err := NewFiles(conf); err == nil {
		t.Error("Expected error from bad path")
	}
}

func TestFilesDirectoryDelete(t *testing.T) {
	tmpDir := t.TempDir()

	tmpFile, err := os.CreateTemp(tmpDir, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tmpFile.WriteString("foo"); err != nil {
		t.Fatal(err)
	}
	if err = tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	exp := map[string]struct{}{
		"foo": {},
	}
	act := map[string]struct{}{}

	conf := NewFilesConfig()
	conf.Path = tmpDir
	conf.DeleteFiles = true

	var f Async
	if f, err = NewFiles(conf); err != nil {
		t.Fatal(err)
	}

	var msg *message.Batch
	if msg, _, err = f.ReadWithContext(context.Background()); err != nil {
		t.Error(err)
	} else {
		resStr := string(msg.Get(0).Get())
		if _, exists := act[resStr]; exists {
			t.Errorf("Received duplicate message: %v", resStr)
		}
		act[resStr] = struct{}{}
	}
	if _, _, err = f.ReadWithContext(context.Background()); err != component.ErrTypeClosed {
		t.Error(err)
	}

	if _, err := os.Stat(path.Join(tmpDir, "f1")); err != nil {
		if !os.IsNotExist(err) {
			t.Errorf("Expected deleted file, received: %v", err)
		}
	} else {
		t.Error("Expected deleted file")
	}

	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------
