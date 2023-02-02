package dbmate

import (
	"database/sql"
	"io"
	"net/url"

	"github.com/amacneil/dbmate/pkg/dbutil"
)

// Driver provides top level database functions
type Driver interface {
	Open() (*sql.DB, error)
	DatabaseExists() (bool, error)
	CreateDatabase() error
	DropDatabase() error
	DumpSchema(*sql.DB) ([]byte, error)
	MigrationsTableExists(*sql.DB) (bool, error)
	CreateMigrationsTable(*sql.DB) error
	SelectMigrations(*sql.DB, int) (map[string]bool, error)
	InsertMigration(dbutil.Transaction, string) error
	DeleteMigration(dbutil.Transaction, string) error
	Ping() error
	GetWildcards() map[string]string
}

// DriverConfig holds configuration passed to driver constructors
type DriverConfig struct {
	DatabaseURL         *url.URL
	MigrationsTableName string
	Log                 io.Writer
}

// DriverFunc represents a driver constructor
type DriverFunc func(DriverConfig) Driver

var drivers = map[string]DriverFunc{}

// RegisterDriver registers a driver constructor for a given URL scheme
func RegisterDriver(f DriverFunc, scheme string) {
	drivers[scheme] = f
}
