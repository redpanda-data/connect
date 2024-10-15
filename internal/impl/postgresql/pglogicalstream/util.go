package pglogicalstream

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5/pgconn"
)

func openPgConnectionFromConfig(dbConf pgconn.Config) (*sql.DB, error) {
	var sslMode string
	if dbConf.TLSConfig != nil {
		sslMode = "require"
	} else {
		sslMode = "disable"
	}
	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=%s", dbConf.User,
		dbConf.Password, dbConf.Host, dbConf.Port, dbConf.Database, sslMode,
	)

	return sql.Open("postgres", connStr)
}

func getPostgresVersion(connConfig pgconn.Config) (int, error) {
	conn, err := openPgConnectionFromConfig(connConfig)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to the database: %w", err)
	}

	var versionString string
	err = conn.QueryRow("SHOW server_version").Scan(&versionString)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}

	// Extract the major version number
	re := regexp.MustCompile(`^(\d+)`)
	match := re.FindStringSubmatch(versionString)
	if len(match) < 2 {
		return 0, fmt.Errorf("failed to parse version string: %s", versionString)
	}

	majorVersion, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("failed to convert version to integer: %w", err)
	}

	return majorVersion, nil
}
