package reader

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/cache"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/manager"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/gabs"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	"github.com/siddontang/go-mysql/canal"
)

type testMySQLRow struct {
	createdAt time.Time
	title     string
	enabled   int
}

func TestMySQLIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	// start mysql container with binlog enabled
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "5.7",
		Cmd: []string{
			"--server-id=1234",
			"--log-bin=/var/lib/mysql/mysql-bin.log",
			"--binlog-do-db=test",
			"--binlog-format=ROW",
		},
		Env: []string{
			"MYSQL_DATABASE=test",
			"MYSQL_ROOT_PASSWORD=s3cr3t",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"3306": []docker.PortBinding{
				docker.PortBinding{
					HostPort: "3306",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %v", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	port, err := strconv.ParseInt(resource.GetPort("3306/tcp"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	// bootstrap mysql
	var db *sql.DB
	if err := pool.Retry(func() error {
		db, err = sql.Open("mysql", fmt.Sprintf("root:s3cr3t@tcp(localhost:%d)/test", port))
		if err == nil {
			// create test table
			_, err := db.Exec("CREATE TABLE `test`.`foo` (id integer AUTO_INCREMENT, created_at datetime(6), title varchar(255), enabled tinyint(1), PRIMARY KEY (id))")
			if err != nil {
				db.Close()
				return err
			}
		}
		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}
	defer db.Close()

	config := NewMySQLConfig()
	config.Cache = "memory"
	config.ConsumerID = 1234
	config.Databases = []string{"test"}
	config.Latest = true
	config.Password = "s3cr3t"
	config.Port = uint32(port)
	config.SyncInterval = "5m"
	config.Username = "root"

	t.Run("testMySQLConnect", func(t *testing.T) {
		testMySQLConnect(t, db, config)
	})

	t.Run("testMySQLBatch", func(t *testing.T) {
		testMySQLBatch(t, db, config)
	})

	t.Run("testMySQLDisconnect", func(t *testing.T) {
		testMySQLDisconnect(t, config)
	})
}

func testMySQLConnect(t *testing.T, db *sql.DB, config MySQLConfig) {
	met := metrics.DudType{}
	log := log.New(os.Stdout, log.Config{LogLevel: "DEBUG"})
	mgr, err := manager.New(manager.NewConfig(), nil, log, met)
	if err != nil {
		t.Fatal(err)
	}

	c, err := cache.NewMemory(cache.NewConfig(), mgr, log, metrics.DudType{})

	config.SyncInterval = "1ms"
	r, err := NewMySQL(config, c, log, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.CloseAsync()
		if err := r.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	// insert mysql fixtures
	rows := []testMySQLRow{
		testMySQLRow{time.Now().Truncate(time.Microsecond), "foo", 1},
		testMySQLRow{time.Now().Truncate(time.Microsecond), "bar", 0},
		testMySQLRow{time.Now().Truncate(time.Microsecond), "baz", 1},
	}
	stmt := "INSERT INTO `test`.`foo` (created_at, title, enabled) VALUES (?, ?, ?)"
	for _, row := range rows {
		_, err = db.Exec(stmt, row.createdAt, row.title, row.enabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	// verify inserts
	for i, row := range rows {
		msg, err := r.Read()
		if err != nil {
			t.Error(err)
		} else {
			if length := msg.Len(); length != 1 {
				t.Errorf("expected message %d to have length of %d, got %d", i, 1, length)
			}
			part := msg.Get(0)
			// verify initial metadata
			meta := part.Metadata()
			if exp, act := "mysql-bin.", meta.Get(metaMysqlFile); !strings.HasPrefix(act, exp) {
				t.Errorf("expected meta[%s] to have prefix %s, got %s", metaMysqlFile, exp, act)
			}
			if exp, act := "test", meta.Get(metaMysqlEventSchema); act != exp {
				t.Errorf("expected meta[%s] to equal %s, got %s", metaMysqlEventSchema, exp, act)
			}
			if exp, act := "foo", meta.Get(metaMysqlEventTable); act != exp {
				t.Errorf("expected meta[%s] to equal %s, got %s", metaMysqlEventTable, exp, act)
			}
			if exp, act := canal.InsertAction, meta.Get(metaMysqlEventType); act != exp {
				t.Errorf("expected meta[%s] to equal %s, got %s", metaMysqlEventType, exp, act)
			}

			// verify payload
			data, err := part.JSON()
			if err != nil {
				t.Errorf("unexpected error retrieving part JSON: %v", err)
			}
			record, _ := data.(*MysqlMessage)
			if exp, act := meta.Get(metaMysqlFile), record.File; act != exp {
				t.Errorf("expected message %d to have file %s, got %s", i, exp, act)
			}
			nextPos, _ := strconv.ParseInt(meta.Get(metaMysqlNextPos), 10, 32)
			if uint32(nextPos) <= record.Pos {
				t.Errorf("expected meta[%s] to be greater than %d, got %d", metaMysqlNextPos, record.Pos, nextPos)
			}
			if schema := record.Schema; schema != "test" {
				t.Errorf("expected message %d to have schema %s, got %s", i, "test", schema)
			}
			if table := record.Table; table != "foo" {
				t.Errorf("expected message %d to have table %s, got %s", i, "foo", table)
			}
			if typ := record.Type; typ != canal.InsertAction {
				t.Errorf("expected message %d to have type %s, got %s", i, canal.InsertAction, typ)
			}
			if ts := record.Timestamp; time.Now().Sub(ts) > time.Second*5 {
				t.Errorf("expected message %d to have timestamp within the last five seconds", i)
			}
			keys, err := gabs.ParseJSON(record.Keys)
			if err != nil {
				t.Error(err)
			} else if id, ok := keys.S("id").Data().(float64); !ok || int(id) != i+1 {
				t.Errorf("expected message %d to have 'key.id' of %d, got %f", i, i+1, id)
			}
			if exp, act := meta.Get(metaMysqlEventKeys), base64.StdEncoding.EncodeToString(record.Keys); act != exp {
				t.Errorf("expected meta[%s] to equal %s, got %s", metaMysqlEventKeys, exp, act)
			}
			if string(record.Row.Before) != "null" {
				t.Errorf("expected message %d to have null before image, got %v", i, record.Row.Before)
			}
			image, _ := gabs.ParseJSON(part.Get())
			if id := image.S("row", "after", "id").Data().(float64); int(id) != i+1 {
				t.Errorf("expected message %d to have id of %d, got %f", i, i+1, id)
			}
			createdAt, err := time.Parse(time.RFC3339Nano, image.S("row", "after", "created_at").Data().(string))
			if err != nil {
				t.Errorf("Failed to parse message %d created_at timestamp: %v", i, err)
			} else if !createdAt.Equal(row.createdAt) {
				t.Errorf("expected message %d to have created_at of %s, got %s", i, row.createdAt, createdAt)
			}
			if title := image.S("row", "after", "title").Data().(string); title != row.title {
				t.Errorf("expected message %d to have title %s, got %s", i, row.title, title)
			}
			if enabled := image.S("row", "after", "enabled").Data().(float64); int(enabled) != row.enabled {
				t.Errorf("expected message %d to have enabled %d, got %d", i, row.enabled, int(enabled))
			}
		}
		if err := r.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	// modify or delete records
	modifications := []struct {
		stmt   string
		id     float64
		action string
		title  string
	}{
		{"UPDATE `test`.`foo` SET title = 'qux' WHERE id = 1;", 1, canal.UpdateAction, "qux"},
		{"UPDATE `test`.`foo` SET title = 'quux' WHERE id = 2;", 2, canal.UpdateAction, "quux"},
		{"DELETE FROM `test`.`foo` WHERE id = 3;", 3, canal.DeleteAction, ""},
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, mod := range modifications {
		tx.Exec(mod.stmt)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// verify update & delete
	for i, mod := range modifications {
		msg, err := r.Read()
		if err != nil {
			t.Error(err)
		} else {
			record, err := gabs.ParseJSON(msg.Get(0).Get())
			if err != nil {
				t.Error(err)
				continue
			}
			action, ok := record.S("type").Data().(string)
			if !ok || action != mod.action {
				t.Errorf("expected modification %d to have type %s, got %s", i, mod.action, action)
			}
			if mod.action == canal.UpdateAction {
				title, ok := record.S("row", "after", "title").Data().(string)
				if !ok || title != mod.title {
					t.Errorf("expected modification %d to have title %s, got %s", i, mod.title, title)
				}
			} else {

				if after := record.S("row", "after").Data(); after != nil {
					t.Errorf("expected modification %d to have empty after image, got %s", i, after)
				}
				id, ok := record.S("row", "before", "id").Data().(float64)
				if !ok || id != mod.id {
					t.Errorf("expected modification %d to have id %f, got %f", i, mod.id, id)
				}
			}
		}
		if err := r.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	time.Sleep(time.Millisecond * 2)
	// verify cache
	var pos mysqlPosition
	val, err := c.Get(r.key)
	if err != nil {
		t.Error(err)
	}
	if err := json.Unmarshal(val, &pos); err != nil {
		t.Error(err)
	}
	if pos.ConsumerID != config.ConsumerID {
		t.Errorf("expected cache position to have id of %d, got %d", config.ConsumerID, pos.ConsumerID)
	}
	if len(pos.Log) == 0 {
		t.Error("expected cache position to have log name")
	}
	if pos.Position == 0 {
		t.Error("expected cache position to have non-zero log position")
	}
	if time.Now().Sub(pos.LastSyncedAt) > time.Second {
		t.Error("expected cache position to have been synced within the last econd")
	}
}

func testMySQLBatch(t *testing.T, db *sql.DB, config MySQLConfig) {
	config.BatchSize = 3
	met := metrics.DudType{}
	log := log.New(os.Stdout, log.Config{LogLevel: "DEBUG"})
	mgr, err := manager.New(manager.NewConfig(), nil, log, met)
	if err != nil {
		t.Fatal(err)
	}

	c, err := cache.NewMemory(cache.NewConfig(), mgr, log, metrics.DudType{})

	r, err := NewMySQL(config, c, log, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.CloseAsync()
		if err := r.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	// insert mysql fixtures

	rows := []testMySQLRow{
		testMySQLRow{time.Now().Truncate(time.Microsecond), "quuz", 1},
		testMySQLRow{time.Now().Truncate(time.Microsecond), "corge", 0},
		testMySQLRow{time.Now().Truncate(time.Microsecond), "grault", 1},
	}
	stmt := "INSERT INTO `test`.`foo` (created_at, title, enabled) VALUES (?, ?, ?)"
	for _, row := range rows {
		_, err = db.Exec(stmt, row.createdAt, row.title, row.enabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	// verify batch
	msg, err := r.Read()
	if err != nil {
		t.Error(err)
	} else {
		if length := msg.Len(); length != 3 {
			t.Fatalf("expected message to have length of %d, got %d", 3, length)
		}
		for i, row := range rows {
			part, err := msg.Get(i).JSON()
			if err != nil {
				t.Error(err)
			} else {
				record, _ := part.(*MysqlMessage)
				if action := record.Type; action != canal.InsertAction {
					t.Errorf("expected part %d to have type %s, got %s", i, canal.InsertAction, action)
				}
				if string(record.Row.Before) != "null" {
					t.Errorf("expected part %d to have null before image, got %v", i, record.Row.Before)
				}
				after, err := gabs.ParseJSON(record.Row.After)
				if err != nil {
					t.Error(err)
				}
				if title, ok := after.S("title").Data().(string); !ok || title != row.title {
					t.Errorf("expected part %d to have title %s, got %s", i, row.title, title)
				}
			}
		}
	}
}

func testMySQLDisconnect(t *testing.T, config MySQLConfig) {
	met := metrics.DudType{}
	log := log.New(os.Stdout, log.Config{LogLevel: "DEBUG"})
	mgr, err := manager.New(manager.NewConfig(), nil, log, met)
	if err != nil {
		t.Fatal(err)
	}

	c, err := cache.NewMemory(cache.NewConfig(), mgr, log, metrics.DudType{})

	r, err := NewMySQL(config, c, log, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Connect(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 3)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		r.CloseAsync()
		if err := r.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	if _, err = r.Read(); err != types.ErrTypeClosed && err != types.ErrNotConnected {
		t.Errorf("wrong error: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}
