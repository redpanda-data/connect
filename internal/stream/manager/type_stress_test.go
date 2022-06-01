package manager_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	bmanager "github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/stream"
	"github.com/benthosdev/benthos/v4/internal/stream/manager"

	// Import pure components for tests
	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestTypeUnderStress(t *testing.T) {
	t.Skip("Skipping long running stress test")

	res, err := bmanager.New(bmanager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(res)

	conf := stream.NewConfig()
	conf.Input.Type = "generate"
	conf.Input.Generate.Count = 3
	conf.Input.Generate.Interval = "1us"
	conf.Input.Generate.Mapping = "root.id = uuid_v4()"
	conf.Output.Type = "drop"

	wg := sync.WaitGroup{}
	for j := 0; j < 1000; j++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				streamID := fmt.Sprintf("foo-%v-%v", threadID, i)
				require.NoError(t, mgr.Create(streamID, conf))

				assert.Eventually(t, func() bool {
					details, err := mgr.Read(streamID)
					return err == nil && !details.IsRunning()
				}, time.Second, time.Millisecond*50)

				require.NoError(t, mgr.Delete(streamID, time.Second))
			}
		}(j)
	}

	wg.Wait()
	require.NoError(t, mgr.Stop(time.Second))
}
