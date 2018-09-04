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

package writer

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/colinmarc/hdfs"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
)

func TestHDFSIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	options := &dockertest.RunOptions{
		Repository:   "cybermaggedon/hadoop",
		Tag:          "2.8.2",
		Hostname:     "localhost",
		ExposedPorts: []string{"9000", "50075", "50070", "50010"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9000/tcp":  {{HostIP: "", HostPort: "9000"}},
			"50070/tcp": {{HostIP: "", HostPort: "50070"}},
			"50075/tcp": {{HostIP: "", HostPort: "50075"}},
			"50010/tcp": {{HostIP: "", HostPort: "50010"}},
		},
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	var client *hdfs.Client

	hosts := []string{"localhost:9000"}
	user := "root"

	if err = pool.Retry(func() error {
		testFile := "/cluster_ready" + time.Now().Format("20060102150405")
		client, err = hdfs.NewClient(hdfs.ClientOptions{
			Addresses: hosts,
			User:      user,
		})
		if err != nil {
			return err
		}
		fw, err := client.Create(testFile)
		if err != nil {
			return err
		}
		_, err = fw.Write([]byte("cluster is ready"))
		if err != nil {
			return err
		}
		err = fw.Close()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	t.Run("TestHDFSConnect", func(th *testing.T) {
		testHDFSConnect(hosts, user, client, th)
	})
}

func testHDFSConnect(hosts []string, user string, client *hdfs.Client, t *testing.T) {
	conf := NewHDFSConfig()
	conf.User = user
	conf.Hosts = hosts
	conf.Directory = "/"
	conf.Path = "${!count:files}-benthos_test.txt"

	h := NewHDFS(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})

	if err := h.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		h.CloseAsync()
		if err := h.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	N := 10

	testMsgs := [][][]byte{}
	for i := 0; i < N; i++ {
		testMsgs = append(testMsgs, [][]byte{
			[]byte(fmt.Sprintf(`{"user":"%v","message":"hello world"}`, i)),
		})
	}
	for i := 0; i < N; i++ {
		if err := h.Write(message.New(testMsgs[i])); err != nil {
			time.Sleep(time.Second * 3000)
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		filePath := fmt.Sprintf("/%v-benthos_test.txt", i+1)
		data, err := client.ReadFile(filePath)
		if err != nil {
			t.Fatalf("Failed to get file '%v': %v", filePath, err)
		}
		if exp, act := string(testMsgs[i][0]), string(data); exp != act {
			t.Errorf("wrong data returned: %v != %v", act, exp)
		}
	}
}
