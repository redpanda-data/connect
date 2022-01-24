package output

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestFanOutBroker(t *testing.T) {
	dir, err := os.MkdirTemp("", "benthos_fan_out_broker_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	outOne, outTwo := NewConfig(), NewConfig()
	outOne.Type, outTwo.Type = TypeFile, TypeFile
	outOne.File.Path = filepath.Join(dir, "one", `foo-${!count("1s")}.txt`)
	outOne.File.Codec = "all-bytes"
	outTwo.File.Path = filepath.Join(dir, "two", `bar-${!count("2s")}.txt`)
	outTwo.File.Codec = "all-bytes"

	procOne, procTwo := processor.NewConfig(), processor.NewConfig()
	procOne.Type, procTwo.Type = processor.TypeBloblang, processor.TypeBloblang
	procOne.Bloblang = `root = "one-" + content()`
	procTwo.Bloblang = `root = "two-" + content()`

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)

	conf := NewConfig()
	conf.Type = TypeBroker
	conf.Broker.Pattern = "fan_out"
	conf.Broker.Outputs = append(conf.Broker.Outputs, outOne, outTwo)

	s, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	inputs := []string{
		"first", "second", "third",
	}
	expFiles := map[string]string{
		"./one/foo-1.txt": "one-first",
		"./one/foo-2.txt": "one-second",
		"./one/foo-3.txt": "one-third",
		"./two/bar-1.txt": "two-first",
		"./two/bar-2.txt": "two-second",
		"./two/bar-3.txt": "two-third",
	}

	for _, input := range inputs {
		testMsg := message.New([][]byte{[]byte(input)})
		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Fatal(res.Error())
			}
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := os.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp != act {
			t.Errorf("Wrong contents for file '%v': %v != %v", k, act, exp)
		}
	}
}

func TestRoundRobinBroker(t *testing.T) {
	dir, err := os.MkdirTemp("", "benthos_round_robin_broker_tests")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	outOne, outTwo := NewConfig(), NewConfig()
	outOne.Type, outTwo.Type = TypeFile, TypeFile
	outOne.File.Path = filepath.Join(dir, "one", `foo-${!count("rrfoo")}.txt`)
	outOne.File.Codec = "all-bytes"
	outTwo.File.Path = filepath.Join(dir, "two", `bar-${!count("rrbar")}.txt`)
	outTwo.File.Codec = "all-bytes"

	procOne, procTwo := processor.NewConfig(), processor.NewConfig()
	procOne.Type, procTwo.Type = processor.TypeBloblang, processor.TypeBloblang
	procOne.Bloblang = `root = "one-" + content()`
	procTwo.Bloblang = `root = "two-" + content()`

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)

	conf := NewConfig()
	conf.Type = TypeBroker
	conf.Broker.Pattern = "round_robin"
	conf.Broker.Outputs = append(conf.Broker.Outputs, outOne, outTwo)

	s, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	})

	inputs := []string{
		"first", "second", "third", "fourth",
	}
	expFiles := map[string]string{
		"./one/foo-1.txt": "one-first",
		"./one/foo-2.txt": "one-third",
		"./two/bar-1.txt": "two-second",
		"./two/bar-2.txt": "two-fourth",
	}

	for _, input := range inputs {
		testMsg := message.New([][]byte{[]byte(input)})
		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Fatal(res.Error())
			}
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := os.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp != act {
			t.Errorf("Wrong contents for file '%v': %v != %v", k, act, exp)
		}
	}
}

func TestGreedyBroker(t *testing.T) {
	dir, err := os.MkdirTemp("", "benthos_broker_greedy_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	outOne, outTwo := NewConfig(), NewConfig()
	outOne.Type, outTwo.Type = TypeFile, TypeFile
	outOne.File.Path = filepath.Join(dir, "one", `foo-${!count("gfoo")}.txt`)
	outOne.File.Codec = "all-bytes"
	outTwo.File.Path = filepath.Join(dir, "two", `bar-${!count("gbar")}.txt`)
	outTwo.File.Codec = "all-bytes"

	procOne, procTwo := processor.NewConfig(), processor.NewConfig()
	procOne.Type, procTwo.Type = processor.TypeBloblang, processor.TypeBloblang
	procOne.Bloblang = `root = "one-" + content()`
	procTwo.Bloblang = `root = "two-" + content()`

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)

	procOne, procTwo = processor.NewConfig(), processor.NewConfig()
	procOne.Type, procTwo.Type = processor.TypeSleep, processor.TypeSleep
	procOne.Sleep.Duration = "50ms"
	procTwo.Sleep.Duration = "50ms"

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)

	conf := NewConfig()
	conf.Type = TypeBroker
	conf.Broker.Pattern = "greedy"
	conf.Broker.Outputs = append(conf.Broker.Outputs, outOne, outTwo)

	s, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	inputs := []string{
		"first", "second", "third", "fourth",
	}
	expFiles := map[string][2]string{
		"./one/foo-1.txt": {"one-first", "one-second"},
		"./one/foo-2.txt": {"one-third", "one-fourth"},
		"./two/bar-1.txt": {"two-first", "two-second"},
		"./two/bar-2.txt": {"two-third", "two-fourth"},
	}

	for _, input := range inputs {
		testMsg := message.New([][]byte{[]byte(input)})
		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Fatal(res.Error())
			}
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := os.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp[0] != act && exp[1] != act {
			t.Errorf("Wrong contents for file '%v': %v != (%v || %v)", k, act, exp[0], exp[1])
		}
	}
}

func TestTryBroker(t *testing.T) {
	dir, err := os.MkdirTemp("", "benthos_try_broker_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	outOne, outTwo, outThree := NewConfig(), NewConfig(), NewConfig()
	outOne.Type, outTwo.Type, outThree.Type = TypeHTTPClient, TypeFile, TypeFile
	outOne.HTTPClient.URL = "http://localhost:11111111/badurl"
	outOne.HTTPClient.NumRetries = 1
	outOne.HTTPClient.Retry = "1ms"
	outTwo.File.Path = filepath.Join(dir, "two", `bar-${!count("tfoo")}-${!count("tbar")}.txt`)
	outTwo.File.Codec = "all-bytes"
	outThree.File.Path = "/dev/null"

	procOne, procTwo, procThree := processor.NewConfig(), processor.NewConfig(), processor.NewConfig()

	procOne.Type, procTwo.Type, procThree.Type = processor.TypeBloblang, processor.TypeBloblang, processor.TypeBloblang
	procOne.Bloblang = `root = "this-should-never-appear %v".format(count("tfoo")) + content()`
	procTwo.Bloblang = `root = "two-" + content()`
	procThree.Bloblang = `root = "this-should-never-appear %v".format(count("tbar")) + content()`

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)
	outThree.Processors = append(outThree.Processors, procThree)

	conf := NewConfig()
	conf.Type = TypeBroker
	conf.Broker.Pattern = "try"
	conf.Broker.Outputs = append(conf.Broker.Outputs, outOne, outTwo, outThree)

	s, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	inputs := []string{
		"first", "second", "third", "fourth",
	}
	expFiles := map[string]string{
		"./two/bar-2-1.txt": "two-first",
		"./two/bar-4-2.txt": "two-second",
		"./two/bar-6-3.txt": "two-third",
		"./two/bar-8-4.txt": "two-fourth",
	}

	for _, input := range inputs {
		testMsg := message.New([][]byte{[]byte(input)})
		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Fatal(res.Error())
			}
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := os.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp != act {
			t.Errorf("Wrong contents for file '%v': %v != %v", k, act, exp)
		}
	}
}
