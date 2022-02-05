package transaction

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkTransactionsChannelN10(b *testing.B) {
	benchmarkTransactionsChannelBased(b, 10)
}

func BenchmarkTransactionsChannelN100(b *testing.B) {
	benchmarkTransactionsChannelBased(b, 100)
}

func BenchmarkTransactionsChannelN1000(b *testing.B) {
	benchmarkTransactionsChannelBased(b, 1000)
}

func BenchmarkTransactionsChannelN10000(b *testing.B) {
	benchmarkTransactionsChannelBased(b, 10000)
}

func BenchmarkTransactionsFuncN10(b *testing.B) {
	benchmarkTransactionsFuncBased(b, 10)
}

func BenchmarkTransactionsFuncN100(b *testing.B) {
	benchmarkTransactionsFuncBased(b, 100)
}

func BenchmarkTransactionsFuncN1000(b *testing.B) {
	benchmarkTransactionsFuncBased(b, 1000)
}

func BenchmarkTransactionsFuncN10000(b *testing.B) {
	benchmarkTransactionsFuncBased(b, 10000)
}

type messageDumb struct {
	raw []byte
}

type transactionChanRes struct {
	m       messageDumb
	resChan chan<- error
}

type transactionFnRes struct {
	m     messageDumb
	resFn func(context.Context, error) error
}

func benchmarkTransactionsChannelBased(b *testing.B, buffered int) {
	tChan := make(chan transactionChanRes)

	b.ReportAllocs()

	var wg sync.WaitGroup
	go func() {
		for i := 0; i < b.N; i++ {
			wg.Add(1)

			rChan := make(chan error)
			tChan <- transactionChanRes{
				m:       messageDumb{raw: []byte(fmt.Sprintf("hello world %v", i))},
				resChan: rChan,
			}

			go func() {
				defer wg.Done()
				<-rChan
			}()
		}
		close(tChan)
	}()

	rChans := make([]chan<- error, buffered)
	ackAll := func() {
		for j := 0; j < buffered; j++ {
			if rChans[j] == nil {
				return
			}
			rChans[j] <- nil
		}
	}
	for {
		for j := 0; j < buffered; j++ {
			rChans[j] = nil
		}
		for j := 0; j < buffered; j++ {
			tran, open := <-tChan
			if !open {
				ackAll()
				wg.Wait()
				return
			}
			rChans[j] = tran.resChan
		}
		ackAll()
	}
}

func benchmarkTransactionsFuncBased(b *testing.B, buffered int) {
	tChan := make(chan transactionFnRes)

	b.ReportAllocs()

	var wg sync.WaitGroup
	go func() {
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			tChan <- transactionFnRes{
				m: messageDumb{raw: []byte(fmt.Sprintf("hello world %v", i))},
				resFn: func(c context.Context, e error) error {
					wg.Done()
					return nil
				},
			}
		}
		close(tChan)
	}()

	rFns := make([]func(context.Context, error) error, buffered)
	ackAll := func() {
		for j := 0; j < buffered; j++ {
			if rFns[j] == nil {
				return
			}
			require.NoError(b, rFns[j](context.Background(), nil))
		}
	}
	for {
		for j := 0; j < buffered; j++ {
			rFns[j] = nil
		}
		for j := 0; j < buffered; j++ {
			tran, open := <-tChan
			if !open {
				ackAll()
				wg.Wait()
				return
			}
			rFns[j] = tran.resFn
		}
		ackAll()
	}
}
