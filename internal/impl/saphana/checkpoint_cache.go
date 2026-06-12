package saphana

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/replication"
)

// schemaTableRe validates "SCHEMA.TABLE" — alphanumeric plus _$#, 1–128 chars each segment.
var schemaTableRe = regexp.MustCompile(`^[A-Za-z_$#][A-Za-z0-9_$#]{0,127}\.[A-Za-z_$#][A-Za-z0-9_$#]{0,127}$`)

// CheckpointCacheConfig holds validated configuration.
type CheckpointCacheConfig struct {
	tableName   string
	createStmts []string // DDL statements to run once on init (was: createTableSQL string)
	upsertSQL   string
	selectSQL   string
}

// NewCheckpointCacheConfig validates tableName and pre-builds SQL.
func NewCheckpointCacheConfig(tableName string) (CheckpointCacheConfig, error) {
	if !schemaTableRe.MatchString(tableName) {
		return CheckpointCacheConfig{}, fmt.Errorf(
			"checkpoint table name %q must be SCHEMA.TABLE with alphanumeric/underscore identifiers", tableName)
	}
	c := CheckpointCacheConfig{tableName: tableName}
	schemaName := strings.Split(tableName, ".")[0]
	c.createStmts = []string{
		fmt.Sprintf(`DO BEGIN
    DECLARE schema_exists CONDITION FOR SQL_ERROR_CODE 386;
    DECLARE EXIT HANDLER FOR schema_exists BEGIN END;
    EXEC 'CREATE SCHEMA %s';
END`, schemaName),
		fmt.Sprintf(`DO BEGIN
    DECLARE tbl_exists CONDITION FOR SQL_ERROR_CODE 288;
    DECLARE EXIT HANDLER FOR tbl_exists BEGIN END;
    EXEC 'CREATE COLUMN TABLE %s (
        CACHE_KEY VARCHAR(255) NOT NULL PRIMARY KEY,
        CACHE_VAL BIGINT NOT NULL,
        UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )';
END`, tableName),
	}
	c.upsertSQL = fmt.Sprintf(`
		UPSERT %s (CACHE_KEY, CACHE_VAL, UPDATED_AT)
		VALUES (?, ?, CURRENT_TIMESTAMP)
		WITH PRIMARY KEY`, tableName)
	c.selectSQL = fmt.Sprintf(
		`SELECT CACHE_VAL FROM %s WHERE CACHE_KEY = ?`, tableName)
	return c, nil
}

// UpsertSQL returns the pre-built UPSERT statement.
func (c CheckpointCacheConfig) UpsertSQL() string { return c.upsertSQL }

// SelectSQL returns the pre-built SELECT statement.
func (c CheckpointCacheConfig) SelectSQL() string { return c.selectSQL }

// ValidateKey checks the cache key string.
func (CheckpointCacheConfig) ValidateKey(key string) error {
	if key == "" {
		return errors.New("checkpoint cache key must not be empty")
	}
	if len(key) > 255 {
		return fmt.Errorf("checkpoint cache key length %d exceeds 255", len(key))
	}
	return nil
}

// CheckpointCache manages a durable LSN checkpoint in a HANA table.
type CheckpointCache struct {
	cfg CheckpointCacheConfig
	db  *sql.DB
	key string
	// lastSaved is the highest LogPos that has been successfully persisted.
	// Used by SaveIfHigher to avoid a round-trip DB read on every ack delivery.
	lastSaved replication.LogPos
}

// NewCheckpointCache creates and initialises the checkpoint table.
func NewCheckpointCache(ctx context.Context, cfg CheckpointCacheConfig, db *sql.DB, key string) (*CheckpointCache, error) {
	if err := cfg.ValidateKey(key); err != nil {
		return nil, err
	}
	for _, stmt := range cfg.createStmts {
		if _, err := db.ExecContext(ctx, strings.TrimSpace(stmt)); err != nil {
			return nil, fmt.Errorf("creating checkpoint infrastructure for %s: %w", cfg.tableName, err)
		}
	}
	c := &CheckpointCache{cfg: cfg, db: db, key: key}
	// Pre-warm lastSaved from the persisted value so SaveIfHigher works
	// correctly on first call without a separate Load().
	if pos, err := c.Load(ctx); err == nil {
		c.lastSaved = pos
	}
	return c, nil
}

// Load returns the last persisted LogPos, or LogPos(0) if none exists.
func (c *CheckpointCache) Load(ctx context.Context) (replication.LogPos, error) {
	var val uint64
	err := c.db.QueryRowContext(ctx, strings.TrimSpace(c.cfg.selectSQL), c.key).Scan(&val)
	if errors.Is(err, sql.ErrNoRows) {
		return replication.LogPos(0), nil
	}
	if err != nil {
		return 0, fmt.Errorf("loading checkpoint for key %q: %w", c.key, err)
	}
	return replication.NewLogPos(val), nil
}

// SaveIfHigher persists pos only if it advances the current checkpoint.
// This prevents out-of-order ack delivery from regressing the checkpoint.
// Uses an in-memory high-water mark to avoid a DB round-trip on every ack.
// Confirmed issue: HANA 2.00.088.00 SPS08 production pattern.
func (c *CheckpointCache) SaveIfHigher(ctx context.Context, pos replication.LogPos) error {
	if pos.IsNull() {
		return nil
	}
	if !c.lastSaved.IsNull() && uint64(pos) <= uint64(c.lastSaved) {
		return nil // already at or past this position — no DB round-trip needed
	}
	if err := c.Save(ctx, pos); err != nil {
		return err
	}
	c.lastSaved = pos
	return nil
}

// Save persists a LogPos. A null (zero) LogPos is a no-op.
func (c *CheckpointCache) Save(ctx context.Context, pos replication.LogPos) error {
	if pos.IsNull() {
		return nil
	}
	if _, err := c.db.ExecContext(ctx, strings.TrimSpace(c.cfg.upsertSQL), c.key, uint64(pos)); err != nil {
		return fmt.Errorf("saving checkpoint %s for key %q: %w", pos, c.key, err)
	}
	return nil
}
