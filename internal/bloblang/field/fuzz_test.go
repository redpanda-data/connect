// +build fuzz

package field

import (
	"runtime"
	"sync"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
)

func TestParserFuzz(t *testing.T) {
	tNow := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			f := fuzz.New().MaxDepth(100000).NilChance(0)
			for {
				if time.Since(tNow) > (time.Minute * 5) {
					break
				}
				var input string
				f.Fuzz(&input)
				e, err := parse(input)
				if err == nil && e == nil {
					t.Errorf("Both returns were nil from: %v", input)
				}
			}
		}()
	}
	wg.Wait()
}
