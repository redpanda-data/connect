package pipeline_test

import (
	"reflect"
	"testing"
	"time"

	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/response"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestProcCtor(t *testing.T) {
	firstProc := processor.NewConfig()
	firstProc.Type = "bounds_check"
	firstProc.BoundsCheck.MinPartSize = 5

	secondProc := processor.NewConfig()
	secondProc.Type = "insert_part"
	secondProc.InsertPart.Content = "1"
	secondProc.InsertPart.Index = 0

	conf := pipeline.NewConfig()
	conf.Processors = append(conf.Processors, firstProc)

	pipe, err := pipeline.New(
		conf, nil,
		log.Noop(),
		metrics.Noop(),
		func() (iprocessor.V1, error) {
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

	tChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	if err = pipe.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case tChan <- message.NewTransaction(
		message.QuickBatch([][]byte{[]byte("foo bar baz")}), resChan,
	):
	}

	var tran message.Transaction
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
			t.Error("timed out")
		case tran.ResponseChan <- response.NewError(nil):
		}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case res := <-resChan:
		if res.AckError() != nil {
			t.Error(res.AckError())
		}
	}

	pipe.CloseAsync()
	if err = pipe.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
