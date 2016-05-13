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

package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/jeffail/benthos/types"
	"github.com/jeffail/benthos/util/test"
)

//--------------------------------------------------------------------------------------------------

func main() {
	runtime.GOMAXPROCS(1)

	var address, path string
	flag.StringVar(&address, "addr", "localhost:1235", "Address to host message receiver at")
	flag.StringVar(&path, "path", "/post", "Path to the POST resource")

	flag.Parse()

	fmt.Println("This is a benchmarking utility for benthos.")
	fmt.Println("Make sure you are running benthos with the ./test/http.yaml config.")

	// Make server
	mux := http.NewServeMux()

	benchChan := test.StartPrintingBenchmarks(time.Second, 0)
	<-time.After(time.Second)

	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			fmt.Printf("| Error: Wrong method, POST != %v\n", r.Method)
			http.Error(w, "Wrong method", 400)
			return
		}

		bytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("| Error reading message: %v\n", err)
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		msg, err := types.FromBytes(bytes)
		if err != nil {
			fmt.Printf("| Error reading message: %v\n", err)
			http.Error(w, "Error", 400)
			return
		}

		if bench, err := test.BenchFromMessage(msg); err == nil {
			benchChan <- bench
		} else {
			fmt.Printf("| Error: Wrong message format: %v\n", err)
			http.Error(w, "Error", 400)
		}
	})

	go func() {
		err := http.ListenAndServe(address, mux)
		panic(err)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
	}

	go func() {
		<-time.After(time.Second * 10)
		panic(errors.New("Timed out waiting for processes to end"))
	}()

	close(benchChan)
	<-time.After(time.Second) // Wait for one final tick.
}

//--------------------------------------------------------------------------------------------------
