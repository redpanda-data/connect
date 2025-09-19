// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build cgo

package tigerbeetle

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	tb "github.com/tigerbeetle/tigerbeetle-go"
	tb_types "github.com/tigerbeetle/tigerbeetle-go/pkg/types"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

const (
	messageCount = 5_000

	connectorYaml = `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ %s ]
  progress_cache: foocache
`
	cacheYaml = `
label: foocache
file:
  directory: %s`
)

func setupTestWithTigerBeetle(t *testing.T, version string) (tb.Client, []string) {
	t.Parallel()
	integration.CheckSkip(t)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:  "ghcr.io/tigerbeetle/tigerbeetle",
		Tag:         version,
		SecurityOpt: []string{"seccomp=unconfined"}, // Required to allow io_uring syscalls.
		Entrypoint:  []string{"sh"},
		Cmd: []string{
			"-c",
			"" +
				"./tigerbeetle format --cluster=0 --replica-count=1 --replica=0 ./0_0.tigerbeetle;" +
				"./tigerbeetle start --addresses=0.0.0.0:3000 --experimental --development ./0_0.tigerbeetle;",
		},
		ExposedPorts: []string{"3000/tcp"},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	time.Sleep(time.Second * 1)
	port := resource.GetPort("3000/tcp")
	addresses := []string{port}
	t.Logf("TigerBeetle running at %s", addresses[0])

	client, err := tb.NewClient(tb_types.ToUint128(0), addresses)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
	})

	return client, addresses
}

func TestIntegrationTigerBeetle(t *testing.T) {
	// Clients are forward compatible with new clusters, but **not** backward compatible,
	// so the oldest supported TigerBeetle cluster must be pinned to the client version
	// used in the connector.
	versions := []string{
		"0.16.57",
		"latest",
	}

	for _, version := range versions {
		t.Run(version, func(t *testing.T) {
			client, addresses := setupTestWithTigerBeetle(t, version)
			connectorConf := fmt.Sprintf(connectorYaml, strings.Join(addresses, ","))
			cacheConf := fmt.Sprintf(cacheYaml, t.TempDir())

			streamOutBuilder := service.NewStreamBuilder()
			require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
			require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
			require.NoError(t, streamOutBuilder.AddInputYAML(connectorConf))

			messages := make([]*service.Message, 0, messageCount)
			require.Empty(t, messages)
			var outBatchMut sync.Mutex
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(
				func(_ context.Context, messageBatch service.MessageBatch) error {
					outBatchMut.Lock()
					defer outBatchMut.Unlock()
					for _, message := range messageBatch {
						messages = append(messages, message.Copy())
					}
					return nil
				}),
			)

			streamOut, err := streamOutBuilder.Build()
			require.NoError(t, err)
			go func() {
				err = streamOut.Run(t.Context())
				require.NoError(t, err)
			}()

			// Creating accounts:
			accounts := make(map[tb_types.Uint128]tb_types.Account)
			accountA := tb_types.ToUint128(1)
			accountB := tb_types.ToUint128(2)
			accounts[accountA] = tb_types.Account{
				ID:          accountA,
				UserData128: tb_types.ToUint128(1000),
				UserData64:  100,
				UserData32:  10,
				Ledger:      1,
				Code:        10,
			}
			accounts[accountB] = tb_types.Account{
				ID:          accountB,
				UserData128: tb_types.ToUint128(2000),
				UserData64:  200,
				UserData32:  20,
				Ledger:      1,
				Code:        20,
			}
			createAccountResults, err := client.CreateAccounts([]tb_types.Account{
				accounts[accountA],
				accounts[accountB],
			})
			require.NoError(t, err)
			require.Empty(t, createAccountResults)

			// Creating transfers:
			transfers := make([]tb_types.Transfer, 0, messageCount)
			require.Empty(t, transfers)
			for i := range messageCount {
				transfer := tb_types.Transfer{
					ID:              tb_types.ToUint128(uint64(i + 1)),
					DebitAccountID:  accountA,
					CreditAccountID: accountB,
					Amount:          tb_types.ToUint128(1),
					UserData128:     tb_types.ToUint128(1000),
					UserData64:      100,
					UserData32:      10,
					Ledger:          1,
					Code:            100,
				}
				createTransfersResult, err := client.CreateTransfers([]tb_types.Transfer{transfer})
				require.NoError(t, err)
				require.Empty(t, createTransfersResult)

				transfers = append(transfers, transfer)
			}

			assert.Eventually(t, func() bool {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				return len(messages) == messageCount
			}, time.Minute*1, time.Millisecond*100)

			timestampLast := uint64(0)
			for i, transfer := range transfers {
				debitAccount := accounts[transfer.DebitAccountID]
				creditAccount := accounts[transfer.CreditAccountID]

				message := messages[i]
				timestampMetaStr, ok := message.MetaGet("timestamp")
				require.True(t, ok)

				timestampMeta, err := strconv.ParseUint(timestampMetaStr, 10, 64)
				require.NoError(t, err)

				// Timestamps must be increasing.
				require.Greater(t, timestampMeta, timestampLast)
				timestampLast = timestampMeta

				// Checking metadata:
				eventType, ok := message.MetaGet("event_type")
				require.True(t, ok)
				require.Equal(t, "single_phase", eventType)

				ledger, ok := message.MetaGet("ledger")
				require.True(t, ok)
				require.Equal(t, strconv.FormatUint(uint64(transfer.Ledger), 10), ledger)

				transferCode, ok := message.MetaGet("transfer_code")
				require.True(t, ok)
				require.Equal(t, strconv.FormatUint(uint64(transfer.Code), 10), transferCode)

				debitAccountCode, ok := message.MetaGet("debit_account_code")
				require.True(t, ok)
				require.Equal(t,
					strconv.FormatUint(uint64(debitAccount.Code), 10),
					debitAccountCode,
				)

				creditAccountCode, ok := message.MetaGet("credit_account_code")
				require.True(t, ok)
				require.Equal(t,
					strconv.FormatUint(uint64(creditAccount.Code), 10),
					creditAccountCode,
				)

				content, err := message.AsBytes()
				require.NoError(t, err)

				// Message content:
				var changeEvent JsonChangeEvent
				require.NoError(t, json.Unmarshal(content, &changeEvent))

				timestampEvent, err := strconv.ParseUint(changeEvent.Transfer.Timestamp, 10, 64)
				require.NoError(t, err)
				require.Equal(t, timestampMeta, timestampEvent)

				// Assert Transfer:
				require.Equal(t, uint128ToString(transfer.ID), changeEvent.Transfer.ID)
				require.Equal(t,
					uint128ToString(transfer.DebitAccountID),
					changeEvent.DebitAccount.ID,
				)
				require.Equal(t,
					uint128ToString(transfer.CreditAccountID),
					changeEvent.CreditAccount.ID,
				)
				require.Equal(t,
					uint128ToString(transfer.Amount),
					changeEvent.Transfer.Amount,
				)
				require.Equal(t,
					uint128ToString(transfer.PendingID),
					changeEvent.Transfer.PendingID,
				)
				require.Equal(t,
					uint128ToString(transfer.UserData128),
					changeEvent.Transfer.UserData128,
				)
				require.Equal(t,
					strconv.FormatUint(transfer.UserData64, 10),
					changeEvent.Transfer.UserData64,
				)
				require.Equal(t, transfer.UserData32, changeEvent.Transfer.UserData32)
				require.Equal(t, transfer.Timeout, changeEvent.Transfer.Timeout)
				require.Equal(t, transfer.Ledger, changeEvent.Ledger)
				require.Equal(t, transfer.Code, changeEvent.Transfer.Code)
				require.Equal(t, transfer.Flags, changeEvent.Transfer.Flags)
				timestampTransfer, err := strconv.ParseUint(changeEvent.Transfer.Timestamp, 10, 64)
				require.NoError(t, err)
				require.LessOrEqual(t, timestampTransfer, timestampEvent)

				// Assert DebitAccount:
				require.Equal(t,
					uint128ToString(debitAccount.UserData128),
					changeEvent.DebitAccount.UserData128,
				)
				require.Equal(t,
					strconv.FormatUint(debitAccount.UserData64, 10),
					changeEvent.DebitAccount.UserData64,
				)
				require.Equal(t, debitAccount.UserData32, changeEvent.DebitAccount.UserData32)
				require.Equal(t, debitAccount.Ledger, changeEvent.Ledger)
				require.Equal(t, debitAccount.Code, changeEvent.DebitAccount.Code)
				require.Equal(t, debitAccount.Flags, changeEvent.DebitAccount.Flags)
				timestampDR, err := strconv.ParseUint(changeEvent.DebitAccount.Timestamp, 10, 64)
				require.NoError(t, err)
				require.Less(t, timestampDR, timestampTransfer)

				// Assert CreditAccount:
				require.Equal(t,
					uint128ToString(creditAccount.UserData128),
					changeEvent.CreditAccount.UserData128,
				)
				require.Equal(t,
					strconv.FormatUint(creditAccount.UserData64, 10),
					changeEvent.CreditAccount.UserData64,
				)
				require.Equal(t, creditAccount.UserData32, changeEvent.CreditAccount.UserData32)
				require.Equal(t, creditAccount.Ledger, changeEvent.Ledger)
				require.Equal(t, creditAccount.Code, changeEvent.CreditAccount.Code)
				require.Equal(t, creditAccount.Flags, changeEvent.CreditAccount.Flags)
				timestampCR, err := strconv.ParseUint(changeEvent.CreditAccount.Timestamp, 10, 64)
				require.NoError(t, err)
				require.Less(t, timestampCR, timestampTransfer)
			}

			require.NoError(t, streamOut.StopWithin(time.Second*10))
		})
	}
}
