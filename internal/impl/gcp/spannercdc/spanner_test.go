package spannercdc

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/iterator"
)

const (
	testTableName = "PartitionMetadata"
	projectID     = "local-project"
	instanceID    = "local-instance"
	databaseID    = "local-database"
)

type SpannerTestSuite struct {
	suite.Suite
	ctx       context.Context
	container testcontainers.Container
	client    *spanner.Client
	timeout   time.Duration
	dsn       string
}

func TestSpannerTestSuite(t *testing.T) {
	suite.Run(t, new(SpannerTestSuite))
}

func (s *SpannerTestSuite) SetupSuite() {
	image := "gcr.io/cloud-spanner-emulator/emulator"
	ports := []string{"9010/tcp"}
	s.ctx = context.Background()
	s.timeout = time.Second * 1500
	s.dsn = fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	envVars := make(map[string]string)
	var err error
	s.container, err = NewTestContainer(s.ctx, image, envVars, ports, wait.ForLog("gRPC server listening at"))
	s.NoError(err)

	mappedPort, err := s.container.MappedPort(s.ctx, "9010")
	s.NoError(err)
	hostIP, err := s.container.Host(s.ctx)
	s.NoError(err)
	hostPort := fmt.Sprintf("%s:%s", hostIP, mappedPort.Port())

	os.Setenv("SPANNER_EMULATOR_HOST", hostPort)

	s.createInstance() // create instance
	s.createDatabase() // create database
}

func (s *SpannerTestSuite) TearDownSuite() {
	if s.container != nil {
		err := s.container.Terminate(s.ctx)
		s.NoError(err)
	}
}

func (s *SpannerTestSuite) AfterTest(suiteName, testName string) {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *SpannerTestSuite) createInstance() {
	instanceAdminClient, err := instance.NewInstanceAdminClient(s.ctx)
	s.NoError(err)
	defer instanceAdminClient.Close()

	op, err := instanceAdminClient.CreateInstance(s.ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + projectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      "emulator-config",
			DisplayName: instanceID,
			NodeCount:   1,
		},
	})
	s.NoError(err)

	_, err = op.Wait(s.ctx)
	s.NoError(err)
}

func (s *SpannerTestSuite) createDatabase() {
	databaseAdminClient, err := database.NewDatabaseAdminClient(s.ctx)
	s.NoError(err)
	defer databaseAdminClient.Close()

	op, err := databaseAdminClient.CreateDatabase(s.ctx, &databasepb.CreateDatabaseRequest{
		Parent:          "projects/" + projectID + "/instances/" + instanceID,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	s.NoError(err)
	_, err = op.Wait(s.ctx)
	s.NoError(err)
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_CreateTableIfNotExists() {
	ctx := context.Background()
	var err error
	s.client, err = spanner.NewClient(ctx, s.dsn)
	s.NoError(err)

	storage := &SpannerPartitionStorage{
		client:    s.client,
		tableName: "CreateTableIfNotExists",
	}

	err = storage.CreateTableIfNotExists(ctx)
	s.NoError(err)

	iter := s.client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), []string{columnPartitionToken})
	defer iter.Stop()

	if _, err := iter.Next(); err != iterator.Done {
		s.T().Errorf("Read from %s after SpannerPartitionStorage.CreateTableIfNotExists() = %v, want %v", storage.tableName, err, iterator.Done)
	}

	existsTable, err := existsTable(ctx, s.client, storage.tableName)
	s.NoError(err)
	if !existsTable {
		s.T().Errorf("SpannerPartitionStorage.existsTable() = %v, want %v", existsTable, false)
	}
}

func existsTable(ctx context.Context, client *spanner.Client, tableName string) (bool, error) {
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT 1 FROM information_schema.tables WHERE table_catalog = '' AND table_schema = '' AND table_name = @tableName",
		Params: map[string]interface{}{
			"tableName": tableName,
		},
	})
	defer iter.Stop()

	if _, err := iter.Next(); err != nil {
		if err == iterator.Done {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (s *SpannerTestSuite) setupSpannerPartitionStorage(ctx context.Context, tableName string) *SpannerPartitionStorage {
	var err error
	s.client, err = spanner.NewClient(ctx, s.dsn)
	s.NoError(err)

	storage := &SpannerPartitionStorage{
		client:    s.client,
		tableName: tableName,
	}

	err = storage.CreateTableIfNotExists(ctx)
	s.NoError(err)

	return storage
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_InitializeRootPartition() {
	ctx := context.Background()
	storage := s.setupSpannerPartitionStorage(ctx, "InitializeRootPartition")

	tests := map[string]struct {
		startTimestamp    time.Time
		endTimestamp      time.Time
		heartbeatInterval time.Duration
		want              PartitionMetadata
	}{
		"one": {
			startTimestamp:    time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			endTimestamp:      time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
			heartbeatInterval: 10 * time.Second,
			want: PartitionMetadata{
				PartitionToken:  RootPartitionToken,
				ParentTokens:    []string{},
				StartTimestamp:  time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				EndTimestamp:    time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
				HeartbeatMillis: 10000,
				State:           StateCreated,
				Watermark:       time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		"two": {
			startTimestamp:    time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
			endTimestamp:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			heartbeatInterval: time.Hour,
			want: PartitionMetadata{
				PartitionToken:  RootPartitionToken,
				ParentTokens:    []string{},
				StartTimestamp:  time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
				EndTimestamp:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				HeartbeatMillis: 3600000,
				State:           StateCreated,
				Watermark:       time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
			},
		},
	}
	for name, test := range tests {
		s.Run(name, func() {
			if err := storage.InitializeRootPartition(ctx, test.startTimestamp, test.endTimestamp, test.heartbeatInterval); err != nil {
				s.T().Errorf("InitializeRootPartition(%q, %q, %q): %v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, err)
				return
			}

			columns := []string{columnPartitionToken, columnParentTokens, columnStartTimestamp, columnEndTimestamp, columnHeartbeatMillis, columnState, columnWatermark}
			row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{RootPartitionToken}, columns)
			if err != nil {
				s.T().Errorf("InitializeRootPartition(%q, %q, %q): %v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, err)
				return
			}

			got := PartitionMetadata{}
			if err := row.ToStruct(&got); err != nil {
				s.T().Errorf("InitializeRootPartition(%q, %q, %q): %v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, err)
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				s.T().Errorf("InitializeRootPartition(%q, %q, %q): got = %+v, want %+v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, got, test.want)
			}
		})
	}
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_Read() {
	ctx := context.Background()
	storage := s.setupSpannerPartitionStorage(ctx, "Read")

	timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	insert := func(token string, start time.Time, state State) *spanner.Mutation {
		return spanner.InsertMap(storage.tableName, map[string]interface{}{
			columnPartitionToken:  token,
			columnParentTokens:    []string{},
			columnStartTimestamp:  start,
			columnEndTimestamp:    time.Time{},
			columnHeartbeatMillis: 0,
			columnState:           state,
			columnWatermark:       start,
			columnCreatedAt:       spanner.CommitTimestamp,
		})
	}

	_, err := storage.client.Apply(ctx, []*spanner.Mutation{
		insert("created1", timestamp, StateCreated),
		insert("created2", timestamp.Add(-2*time.Second), StateCreated),
		insert("scheduled", timestamp.Add(time.Second), StateScheduled),
		insert("running", timestamp.Add(2*time.Second), StateRunning),
		insert("finished", timestamp.Add(-time.Second), StateFinished),
	})
	s.NoError(err)

	s.Run("GetUnfinishedMinWatermarkPartition", func() {
		got, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		s.NoError(err)

		want := "created2"
		if got.PartitionToken != want {
			s.T().Errorf("GetUnfinishedMinWatermarkPartition(ctx) = %v, want = %v", got.PartitionToken, want)
		}
	})

	s.Run("GetInterruptedPartitions", func() {
		partitions, err := storage.GetInterruptedPartitions(ctx)
		s.NoError(err)
		got := []string{}
		for _, p := range partitions {
			got = append(got, p.PartitionToken)
		}

		want := []string{"scheduled", "running"}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("GetInterruptedPartitions(ctx) = %+v, want = %+v", got, want)
		}
	})

	s.Run("GetSchedulablePartitions", func() {
		partitions, err := storage.GetSchedulablePartitions(ctx, timestamp)
		s.NoError(err)

		got := []string{}
		for _, p := range partitions {
			got = append(got, p.PartitionToken)
		}

		want := []string{"created1"}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("GetSchedulablePartitions(ctx, %q) = %+v, want = %+v", timestamp, got, want)
		}
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_AddChildPartitions() {
	ctx := context.Background()
	storage := s.setupSpannerPartitionStorage(ctx, "AddChildPartitions")

	childStartTimestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endTimestamp := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	var heartbeatMillis int64 = 10000

	parent := &PartitionMetadata{
		PartitionToken:  "parent1",
		ParentTokens:    []string{},
		StartTimestamp:  time.Time{},
		EndTimestamp:    endTimestamp,
		HeartbeatMillis: heartbeatMillis,
		State:           StateRunning,
		Watermark:       time.Time{},
	}
	record := &ChildPartitionsRecord{
		StartTimestamp: childStartTimestamp,
		ChildPartitions: []*ChildPartition{
			{Token: "token1", ParentPartitionTokens: []string{"parent1"}},
			{Token: "token2", ParentPartitionTokens: []string{"parent1"}},
		},
	}
	err := storage.AddChildPartitions(ctx, parent, record)
	s.NoError(err)

	columns := []string{columnPartitionToken, columnParentTokens, columnStartTimestamp, columnEndTimestamp, columnHeartbeatMillis, columnState, columnWatermark}

	got := []PartitionMetadata{}
	err = storage.client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), columns).Do(func(r *spanner.Row) error {
		p := PartitionMetadata{}
		if err := r.ToStruct(&p); err != nil {
			return err
		}
		got = append(got, p)
		return nil
	})
	s.NoError(err)

	want := []PartitionMetadata{
		{
			PartitionToken:  "token1",
			ParentTokens:    []string{"parent1"},
			StartTimestamp:  childStartTimestamp,
			EndTimestamp:    endTimestamp,
			HeartbeatMillis: heartbeatMillis,
			State:           StateCreated,
			Watermark:       childStartTimestamp,
		},
		{
			PartitionToken:  "token2",
			ParentTokens:    []string{"parent1"},
			StartTimestamp:  childStartTimestamp,
			EndTimestamp:    endTimestamp,
			HeartbeatMillis: heartbeatMillis,
			State:           StateCreated,
			Watermark:       childStartTimestamp,
		},
	}
	if !reflect.DeepEqual(got, want) {
		s.T().Errorf("GetSchedulablePartitions(ctx, %+v, %+v): got = %+v, want %+v", parent, record, got, want)
	}
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_Update() {
	ctx := context.Background()
	storage := s.setupSpannerPartitionStorage(ctx, "Update")

	create := func(token string) *PartitionMetadata {
		return &PartitionMetadata{
			PartitionToken:  token,
			ParentTokens:    []string{},
			StartTimestamp:  time.Time{},
			EndTimestamp:    time.Time{},
			HeartbeatMillis: 0,
			State:           StateCreated,
			Watermark:       time.Time{},
		}
	}

	insert := func(p *PartitionMetadata) *spanner.Mutation {
		return spanner.InsertMap(storage.tableName, map[string]interface{}{
			columnPartitionToken:  p.PartitionToken,
			columnParentTokens:    p.ParentTokens,
			columnStartTimestamp:  p.StartTimestamp,
			columnEndTimestamp:    p.EndTimestamp,
			columnHeartbeatMillis: p.HeartbeatMillis,
			columnState:           p.State,
			columnWatermark:       p.Watermark,
			columnCreatedAt:       spanner.CommitTimestamp,
		})
	}

	partitions := []*PartitionMetadata{create("token1"), create("token2")}

	_, err := storage.client.Apply(ctx, []*spanner.Mutation{
		insert(partitions[0]),
		insert(partitions[1]),
	})
	s.NoError(err)

	s.Run("UpdateToScheduled", func() {
		err := storage.UpdateToScheduled(ctx, partitions)
		s.NoError(err)

		columns := []string{columnPartitionToken, columnState}

		type partition struct {
			PartitionToken string `spanner:"PartitionToken"`
			State          State  `spanner:"State"`
		}
		got := []partition{}

		err = storage.client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), columns).Do(func(r *spanner.Row) error {
			p := partition{}
			if err := r.ToStruct(&p); err != nil {
				return err
			}
			got = append(got, p)
			return nil
		})
		s.NoError(err)

		want := []partition{
			{PartitionToken: "token1", State: StateScheduled},
			{PartitionToken: "token2", State: StateScheduled},
		}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("UpdateToScheduled(ctx, %+v): got = %+v, want %+v", partitions, got, want)
		}
	})

	s.Run("UpdateToRunning", func() {
		err := storage.UpdateToRunning(ctx, partitions[0])
		s.NoError(err)

		columns := []string{columnPartitionToken, columnState}

		type partition struct {
			PartitionToken string `spanner:"PartitionToken"`
			State          State  `spanner:"State"`
		}

		r, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, columns)
		s.NoError(err)

		got := partition{}
		err = r.ToStruct(&got)
		s.NoError(err)

		want := partition{PartitionToken: "token1", State: StateRunning}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("UpdateToRunning(ctx, %+v): got = %+v, want %+v", partitions[0], got, want)
		}
	})

	s.Run("UpdateToFinished", func() {
		err := storage.UpdateToFinished(ctx, partitions[0])
		s.NoError(err)

		columns := []string{columnPartitionToken, columnState}

		type partition struct {
			PartitionToken string `spanner:"PartitionToken"`
			State          State  `spanner:"State"`
		}

		r, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, columns)
		s.NoError(err)

		got := partition{}
		err = r.ToStruct(&got)
		s.NoError(err)

		want := partition{PartitionToken: "token1", State: StateFinished}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("UpdateToFinished(ctx, %+v): got = %+v, want %+v", partitions[0], got, want)
		}
	})

	s.Run("UpdateWatermark", func() {
		timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

		err := storage.UpdateWatermark(ctx, partitions[0], timestamp)
		s.NoError(err)

		columns := []string{columnPartitionToken, columnWatermark}

		type partition struct {
			PartitionToken string    `spanner:"PartitionToken"`
			Watermark      time.Time `spanner:"Watermark"`
		}

		r, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, columns)
		s.NoError(err)

		got := partition{}
		err = r.ToStruct(&got)
		s.NoError(err)

		want := partition{PartitionToken: "token1", Watermark: timestamp}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("UpdateWatermark(ctx, %+v, %q): got = %+v, want %+v", partitions[0], timestamp, got, want)
		}
	})
}
