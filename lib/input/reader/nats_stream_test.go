package reader

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"os"
	"testing"
	"time"
)

func TestNewNATSStreamConnect(t *testing.T) {

	t.Run("TestNewNATSStreamConnectAuth", func(t *testing.T) {
		TestNewNATSStreamConnectAuth(t)
	})

	t.Run("TestNewNATSStreamConnectNoAuth", func(t *testing.T) {
		TestNewNATSStreamConnectNoAuth(t)
	})
	t.Run("TestNewNATSStreamConnectURLAuth", func(t *testing.T) {
		TestNewNATSStreamConnectURLAuth(t)
	})
}

func TestNewNATSStreamConnectAuth(t *testing.T)  {
	conf := NewNATSStreamConfig()
	conf.Username = "root"
	conf.Password = "root"
	conf.ClusterID = "test-cluster"
	conf.URLs = []string{"nats://127.0.0.1:4223"}

	na, err := NewNATSStream(conf,  log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}
	err = na.Connect()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		na.CloseAsync()
		if cErr := na.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()
}

func TestNewNATSStreamConnectURLAuth(t *testing.T)  {
	conf := NewNATSStreamConfig()
	conf.URLs = []string{"nats://root:root@127.0.0.1:4223"}

	na, err := NewNATSStream(conf,  log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}
	err = na.Connect()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		na.CloseAsync()
		if cErr := na.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()
}

func TestNewNATSStreamConnectNoAuth(t *testing.T)  {
	conf := NewNATSStreamConfig()
	conf.URLs = []string{"nats://127.0.0.1:4223"}

	na, err := NewNATSStream(conf,  log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}
	err = na.Connect()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		na.CloseAsync()
		if cErr := na.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()
}