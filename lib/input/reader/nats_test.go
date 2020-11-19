package reader

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"os"
	"testing"
	"time"
)

func TestNewNATSConnect(t *testing.T) {

	t.Run("TestNatsConnectAuth", func(t *testing.T) {
		TestNatsConnectAuth(t)
	})

	t.Run("TestNatsConnectNoAuth", func(t *testing.T) {
		TestNatsConnectNoAuth(t)
	})

}

func TestNatsConnectAuth(t *testing.T)  {
	conf := NewNATSConfig()
	conf.Username = "root"
	conf.Password = "root"
	conf.URLs = []string{"nats://127.0.0.1:4222"}

	na, err := NewNATS(conf,  log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
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

func TestNatsConnectNoAuth(t *testing.T)  {
	conf := NewNATSConfig()
	conf.URLs = []string{"nats://127.0.0.1:4222"}

	na, err := NewNATS(conf,  log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
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