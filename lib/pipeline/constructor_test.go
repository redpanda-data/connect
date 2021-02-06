package pipeline

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestSanitise(t *testing.T) {
	var actObj interface{}
	var act []byte
	var err error

	exp := `{` +
		`"processors":[],` +
		`"threads":10` +
		`}`

	conf := NewConfig()
	conf.Threads = 10
	conf.Processors = nil

	if actObj, err = SanitiseConfig(conf); err != nil {
		t.Fatal(err)
	}
	if act, err = json.Marshal(actObj); err != nil {
		t.Fatal(err)
	}
	if string(act) != exp {
		t.Errorf("Wrong sanitised output: %s != %v", act, exp)
	}

	exp = `{` +
		`"processors":[` +
		`{` +
		`"type":"log",` +
		`"log":{` +
		`"fields":{},` +
		`"fields_mapping":"",` +
		`"level":"INFO",` +
		`"message":""` +
		`}` +
		`},` +
		`{` +
		`"type":"archive",` +
		`"archive":{` +
		`"format":"binary",` +
		`"path":"nope"` +
		`}` +
		`}` +
		`],` +
		`"threads":10` +
		`}`

	proc := processor.NewConfig()
	proc.Type = "log"
	conf.Processors = append(conf.Processors, proc)

	proc = processor.NewConfig()
	proc.Type = "archive"
	proc.Archive.Path = "nope"
	conf.Processors = append(conf.Processors, proc)

	if actObj, err = SanitiseConfig(conf); err != nil {
		t.Fatal(err)
	}
	if act, err = json.Marshal(actObj); err != nil {
		t.Fatal(err)
	}
	if string(act) != exp {
		t.Errorf("Wrong sanitised output: %s != %v", act, exp)
	}
}

func TestProcCtor(t *testing.T) {
	firstProc := processor.NewConfig()
	firstProc.Type = "bounds_check"
	firstProc.BoundsCheck.MinPartSize = 5

	secondProc := processor.NewConfig()
	secondProc.Type = "insert_part"
	secondProc.InsertPart.Content = "1"
	secondProc.InsertPart.Index = 0

	conf := NewConfig()
	conf.Processors = append(conf.Processors, firstProc)

	pipe, err := New(
		conf, nil,
		log.Noop(),
		metrics.Noop(),
		func() (types.Processor, error) {
			return processor.New(
				secondProc, nil,
				log.Noop(),
				metrics.Noop(),
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = pipe.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case tChan <- types.NewTransaction(
		message.New([][]byte{[]byte("foo bar baz")}), resChan,
	):
	}

	var tran types.Transaction
	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case tran = <-pipe.TransactionChan():
	}

	exp := [][]byte{
		[]byte("1"),
		[]byte("foo bar baz"),
	}
	if act := message.GetAllBytes(tran.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong contents: %s != %s", act, exp)
	}

	go func() {
		select {
		case <-time.After(time.Second):
			t.Fatal("timed out")
		case tran.ResponseChan <- response.NewAck():
		}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case res := <-resChan:
		if res.Error() != nil {
			t.Error(res.Error())
		}
	}

	pipe.CloseAsync()
	if err = pipe.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
