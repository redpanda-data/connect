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

package integration

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/colinmarc/hdfs"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
)

func TestHDFSIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()

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
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()
	resource.Expire(900)

	hosts := []string{"localhost:9000"}
	user := "root"

	if err = pool.Retry(func() error {
		testFile := "/cluster_ready" + time.Now().Format("20060102150405")
		client, err := hdfs.NewClient(hdfs.ClientOptions{
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
		_, err = fw.Write([]byte("testing hdfs reader"))
		if err != nil {
			return err
		}
		err = fw.Close()
		if err != nil {
			return err
		}
		client.Remove(testFile)
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("TestHDFSReaderWriterBasic", func(th *testing.T) {
		testHDFSReaderBasic(hosts, user, th)
	})
}

func testHDFSReaderBasic(hosts []string, user string, t *testing.T) {
	wconf := writer.NewHDFSConfig()
	wconf.User = user
	wconf.Hosts = hosts
	wconf.Directory = "/"
	wconf.Path = "${!count:files}-benthos_test.txt"

	w := writer.NewHDFS(wconf, log.Noop(), metrics.Noop())

	if err := w.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		w.CloseAsync()
		if err := w.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	N := 9

	testMsgs := [][][]byte{}

	for i := 0; i < N; i++ {
		testMsgs = append(testMsgs, [][]byte{
			[]byte(fmt.Sprintf(`{"user":"%v","message":"hello world"}`, i)),
		})
	}

	for i := 0; i < N; i++ {
		if err := w.Write(message.New(testMsgs[i])); err != nil {
			time.Sleep(time.Second * 3000)
			t.Fatal(err)
		}
	}

	rconf := reader.NewHDFSConfig()
	rconf.User = user
	rconf.Hosts = hosts
	rconf.Directory = "/"

	r := reader.NewHDFS(rconf, log.Noop(), metrics.Noop())

	if err := r.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		r.CloseAsync()
		if err := r.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	for i, expMsg := range testMsgs {
		msg, err := r.Read()
		if err != nil {
			t.Fatalf("Failed to read message '%v': %v", i, err)
		}
		if act := message.GetAllBytes(msg); !reflect.DeepEqual(expMsg, act) {
			t.Errorf("wrong data returned: %s != %s", act, expMsg)
		}
		fileName := fmt.Sprintf("%v-benthos_test.txt", i+1)
		filePath := fmt.Sprintf("/%v", fileName)
		if exp, act := fileName, msg.Get(0).Metadata().Get("hdfs_name"); exp != act {
			t.Errorf("Wrong metadata returned: %v != %v", act, exp)
		}
		if exp, act := filePath, msg.Get(0).Metadata().Get("hdfs_path"); exp != act {
			t.Errorf("Wrong metadata returned: %v != %v", act, exp)
		}
	}
}
