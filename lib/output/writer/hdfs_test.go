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
)

var (
	hdfsNameNode, hdfsUser string
)

func init() {
	hdfsNameNode = os.Getenv("HDFS_NAMENODE")
	hdfsUser = os.Getenv("HDFS_USER")
}

func TestHDFSIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if hdfsNameNode == "" || hdfsUser == "" {
		t.Skip("HDFS config not specified.")
	}

	hosts := []string{hdfsNameNode}
	user := hdfsUser

	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hosts,
		User:      user,
	})
	if err != nil {
		t.Fatalf("hdfs.NewClient(%s) failed: %v", hdfsNameNode, err)
	}

	t.Run("TestHDFSConnect", func(t *testing.T) {
		testHDFSConnect(hosts, user, client, t)
	})
}

func testHDFSConnect(hosts []string, user string, client *hdfs.Client, t *testing.T) {
	conf := NewHDFSConfig()
	conf.User = user
	conf.Hosts = hosts
	conf.Directory = "/tmp/benthos_test"
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
		filePath := fmt.Sprintf("/tmp/benthos_test/%v-benthos_test.txt", i+1)
		data, err := client.ReadFile(filePath)
		if err != nil {
			t.Fatalf("Failed to get file '%v': %v", filePath, err)
		}
		if exp, act := string(testMsgs[i][0]), string(data); exp != act {
			t.Errorf("wrong data returned: %v != %v", act, exp)
		}
	}
}
