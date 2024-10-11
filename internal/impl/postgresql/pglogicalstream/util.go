package pglogicalstream

import (
	"database/sql"
	"fmt"

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
