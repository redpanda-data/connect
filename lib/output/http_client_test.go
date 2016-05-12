/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package output

import (
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func TestHTTPClientBasic(t *testing.T) {
	nTestLoops := 1000

	sendChan, resultChan := make(chan types.Message), make(chan types.Message, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg types.Message
		defer func() {
			resultChan <- msg
		}()

		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Errorf("Bad media type: %v -> %v", r.Header.Get("Content-Type"), err)
			return
		}

		if strings.HasPrefix(mediaType, "multipart/") {
			msg.Parts = [][]byte{}
			mr := multipart.NewReader(r.Body, params["boundary"])
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					return
				}
				msgBytes, err := ioutil.ReadAll(p)
				if err != nil {
					t.Error(err)
					return
				}
				msg.Parts = append(msg.Parts, msgBytes)
			}
		} else if r.Header.Get("Content-Type") == "application/x-benthos-multipart" {
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			msg, err = types.FromBytes(b)
			if err != nil {
				t.Error(err)
				return
			}
		} else {
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			msg.Parts = [][]byte{b}
		}
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/testpost"

	h, err := NewHTTPClient(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = h.StartReceiving(sendChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := types.Message{Parts: [][]byte{[]byte(testStr)}}

		select {
		case sendChan <- testMsg:
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}

		select {
		case resMsg := <-resultChan:
			if len(resMsg.Parts) != 1 {
				t.Errorf("Wrong # parts: %v != %v", len(resMsg.Parts), 1)
				return
			}
			if exp, actual := testStr, string(resMsg.Parts[0]); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}

		select {
		case res := <-h.ResponseChan():
			if res.Error() != nil {
				t.Error(res.Error())
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := types.Message{Parts: [][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		}}

		select {
		case sendChan <- testMsg:
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}

		select {
		case resMsg := <-resultChan:
			if len(resMsg.Parts) != 2 {
				t.Errorf("Wrong # parts: %v != %v", len(resMsg.Parts), 2)
				return
			}
			if exp, actual := testStr+"PART-A", string(resMsg.Parts[0]); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
			if exp, actual := testStr+"PART-B", string(resMsg.Parts[1]); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}

		select {
		case res := <-h.ResponseChan():
			if res.Error() != nil {
				t.Error(res.Error())
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}
}

//--------------------------------------------------------------------------------------------------
