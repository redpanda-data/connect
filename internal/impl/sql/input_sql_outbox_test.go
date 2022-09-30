package sql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

const testOutboxConfig = `
  driver: postgres
  dsn: postgres://postgres@localhost:5432/postgres?sslmode=disable
  table: outbox_items
  columns:
    - payload
    - created_at
    - id
  where: deliverable_at <= now()
  id_column: id
  retry_count_column: attempt_count
  max_retries: 3
  item_count: 10
  rate_limit: outbox_poll_rate
`

func buildTestInput() (*sqlOutboxInput, error) {
	spec := sqlOutboxInputConfig()

	mgr := service.MockResources(
		service.MockResourcesOptAddRateLimit(
			"outbox_poll_rate",
			func(ctx context.Context) (time.Duration, error) { return 0, nil },
		),
	)

	parsed, err := spec.ParseYAML(testOutboxConfig, nil)
	if err != nil {
		return nil, err
	}

	return newSQLOutboxInputFromConfig(parsed, mgr)
}

func TestSQLOutboxInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	inp, err := buildTestInput()
	require.NoError(t, err)

	inp.db = db

	rows := sqlmock.NewRows([]string{"__benthos_id", "id", "payload", "created_at"}).
		AddRow(1, 1, "post 1", "2022-09-29 16:00:00.000000").
		AddRow(2, 2, "post 2", "2022-09-29 16:00:00.000000").
		AddRow(3, 3, "post 3", "2022-09-29 16:00:00.000000")

	mock.ExpectBegin()
	mock.
		ExpectQuery(strings.ReplaceAll(`
SELECT id as __benthos_id, payload, created_at, id
FROM outbox_items
WHERE attempt_count < \$1 AND deliverable_at <= now\(\)
LIMIT 10
FOR UPDATE SKIP LOCKED
`, "\n", " ")).
		WithArgs(3).
		WillReturnRows(rows)
	mock.
		ExpectExec(`DELETE FROM outbox_items WHERE id in \(\$1, \$2, \$3\)`).
		WithArgs(1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectCommit()

	err = inp.Connect(ctx)
	require.NoError(t, err)

	_, ack, err := inp.ReadBatch(ctx)
	require.NoError(t, err)

	err = ack(ctx, nil)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestSQLOutboxInput_WithOpaqueError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	inp, err := buildTestInput()
	require.NoError(t, err)

	inp.db = db

	rows := sqlmock.NewRows([]string{"__benthos_id", "id", "payload", "created_at"}).
		AddRow(1, 1, "post 1", "2022-09-29 16:00:00.000000").
		AddRow(2, 2, "post 2", "2022-09-29 16:00:00.000000").
		AddRow(3, 3, "post 3", "2022-09-29 16:00:00.000000")

	mock.ExpectBegin()
	mock.
		ExpectQuery(strings.ReplaceAll(`
SELECT id as __benthos_id, payload, created_at, id
FROM outbox_items
WHERE attempt_count < \$1 AND deliverable_at <= now\(\)
LIMIT 10
FOR UPDATE SKIP LOCKED
`, "\n", " ")).
		WithArgs(3).
		WillReturnRows(rows)
	mock.
		ExpectExec(strings.ReplaceAll(`
UPDATE outbox_items
SET attempt_count = attempt_count \+ 1
WHERE id in \(\$1, \$2, \$3\)
  `, "\n", " ")).
		WithArgs(1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectCommit()

	err = inp.Connect(ctx)
	require.NoError(t, err)

	_, ack, err := inp.ReadBatch(ctx)
	require.NoError(t, err)

	err = ack(ctx, errors.New("everything should fail"))
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestSQLOutboxInput_WithPartialFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	inp, err := buildTestInput()
	require.NoError(t, err)

	inp.db = db

	rows := sqlmock.NewRows([]string{"__benthos_id", "id", "payload", "created_at"}).
		AddRow(1, 1, "post 1", "2022-09-29 16:00:00.000000").
		AddRow(2, 2, "post 2", "2022-09-29 16:00:00.000000").
		AddRow(3, 3, "post 3", "2022-09-29 16:00:00.000000")

	mock.ExpectBegin()
	mock.
		ExpectQuery(strings.ReplaceAll(`
SELECT id as __benthos_id, payload, created_at, id
FROM outbox_items
WHERE attempt_count < \$1 AND deliverable_at <= now\(\)
LIMIT 10
FOR UPDATE SKIP LOCKED
`, "\n", " ")).
		WithArgs(3).
		WillReturnRows(rows)
	mock.
		ExpectExec(`DELETE FROM outbox_items WHERE id in \(\$1, \$2\)`).
		WithArgs(1, 3).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.
		ExpectExec(strings.ReplaceAll(`
UPDATE outbox_items
SET attempt_count = attempt_count \+ 1
WHERE id in \(\$1\)
  `, "\n", " ")).
		WithArgs(2).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = inp.Connect(ctx)
	require.NoError(t, err)

	batch, ack, err := inp.ReadBatch(ctx)
	require.NoError(t, err)

	berrs := service.NewBatchError(batch, errors.New("mock batch error"))
	berrs.Failed(1, errors.New("simulated failure"))

	err = ack(ctx, berrs)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestSQLOutboxInput_UnexpectedQueryError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	inp, err := buildTestInput()
	require.NoError(t, err)

	inp.db = db

	mock.ExpectBegin()
	mock.
		ExpectQuery(strings.ReplaceAll(`
SELECT id as __benthos_id, payload, created_at, id
FROM outbox_items
WHERE attempt_count < \$1 AND deliverable_at <= now\(\)
LIMIT 10
FOR UPDATE SKIP LOCKED
`, "\n", " ")).
		WithArgs(3).
		WillReturnError(errors.New("simulated error")) // !!!!!!!
	mock.ExpectRollback()

	err = inp.Connect(ctx)
	require.NoError(t, err)

	_, _, err = inp.ReadBatch(ctx)
	require.Error(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestSQLOutboxInput_UnexpectedExecError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	inp, err := buildTestInput()
	require.NoError(t, err)

	inp.db = db

	rows := sqlmock.NewRows([]string{"__benthos_id", "id", "payload", "created_at"}).
		AddRow(1, 1, "post 1", "2022-09-29 16:00:00.000000").
		AddRow(2, 2, "post 2", "2022-09-29 16:00:00.000000").
		AddRow(3, 3, "post 3", "2022-09-29 16:00:00.000000")

	mock.ExpectBegin()
	mock.
		ExpectQuery(strings.ReplaceAll(`
  SELECT id as __benthos_id, payload, created_at, id
  FROM outbox_items
  WHERE attempt_count < \$1 AND deliverable_at <= now\(\)
  LIMIT 10
  FOR UPDATE SKIP LOCKED
  `, "\n", " ")).
		WithArgs(3).
		WillReturnRows(rows)
	mock.
		ExpectExec(strings.ReplaceAll(`
  UPDATE outbox_items
  SET attempt_count = attempt_count \+ 1
  WHERE id in \(\$1, \$2, \$3\)
    `, "\n", " ")).
		WithArgs(1, 2, 3).
		WillReturnError(errors.New("simulated error")) // !!!!!!!
	mock.ExpectRollback()

	err = inp.Connect(ctx)
	require.NoError(t, err)

	_, ack, err := inp.ReadBatch(ctx)
	require.NoError(t, err)

	err = ack(ctx, errors.New("everything should fail"))
	require.Error(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}
