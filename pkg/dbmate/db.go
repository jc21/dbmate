package dbmate

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/amacneil/dbmate/pkg/dbutil"
)

// DefaultMigrationsDir specifies default directory to find migration files
const DefaultMigrationsDir = "./db/migrations"

// DefaultMigrationsTableName specifies default database tables to record migraitons in
const DefaultMigrationsTableName = "schema_migrations"

// DefaultSchemaFile specifies default location for schema.sql
const DefaultSchemaFile = "./db/schema.sql"

// DefaultWaitInterval specifies length of time between connection attempts
const DefaultWaitInterval = time.Second

// DefaultWaitTimeout specifies maximum time for connection attempts
const DefaultWaitTimeout = 60 * time.Second

// Error codes
var (
	ErrNoMigrationFiles      = errors.New("no migration files found")
	ErrInvalidURL            = errors.New("invalid url, have you set your --url flag or DATABASE_URL environment variable?")
	ErrNoRollback            = errors.New("can't rollback: no migrations have been applied")
	ErrNoRollbackSlaves      = errors.New("can't rollback: slave(s) are out of sync with master")
	ErrCantConnect           = errors.New("unable to connect to database")
	ErrUnsupportedDriver     = errors.New("unsupported driver")
	ErrNoMigrationName       = errors.New("please specify a name for the new migration")
	ErrMigrationAlreadyExist = errors.New("file already exists")
	ErrMigrationDirNotFound  = errors.New("could not find migrations directory")
	ErrMigrationNotFound     = errors.New("can't find migration file")
	ErrCreateDirectory       = errors.New("unable to create directory")
)

// DB allows dbmate actions to be performed on a specified database
type DB struct {
	AutoDumpSchema      bool
	DatabaseURL         *url.URL
	SlaveDatabases      []*url.URL
	MigrationsDir       string
	MigrationsTableName string
	SchemaFile          string
	Verbose             bool
	WaitBefore          bool
	WaitInterval        time.Duration
	WaitTimeout         time.Duration
	Log                 io.Writer
}

// migrationFileRegexp pattern for valid migration files
var migrationFileRegexp = regexp.MustCompile(`^\d.*\.sql$`)

// StatusResult represents an available migration status
type StatusResult struct {
	Filename string
	Applied  bool
}

// New initializes a new dbmate database
func New(databaseURL *url.URL, slaveDatabases []*url.URL) *DB {
	return &DB{
		AutoDumpSchema:      true,
		DatabaseURL:         databaseURL,
		SlaveDatabases:      slaveDatabases,
		MigrationsDir:       DefaultMigrationsDir,
		MigrationsTableName: DefaultMigrationsTableName,
		SchemaFile:          DefaultSchemaFile,
		WaitBefore:          false,
		WaitInterval:        DefaultWaitInterval,
		WaitTimeout:         DefaultWaitTimeout,
		Log:                 os.Stdout,
	}
}

// GetDriver initializes the appropriate database driver
func (db *DB) GetDriver() (Driver, error) {
	if db.DatabaseURL == nil || db.DatabaseURL.Scheme == "" {
		return nil, ErrInvalidURL
	}

	driverFunc := drivers[db.DatabaseURL.Scheme]
	if driverFunc == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDriver, db.DatabaseURL.Scheme)
	}

	config := DriverConfig{
		DatabaseURL:         db.DatabaseURL,
		MigrationsTableName: db.MigrationsTableName,
		Log:                 db.Log,
	}

	return driverFunc(config), nil
}

// GetSlaveDrivers initializes the appropriate slave drivers
func (db *DB) GetSlaveDrivers() ([]Driver, error) {
	if db.SlaveDatabases == nil {
		return nil, nil
	}

	d := make([]Driver, 0)
	for _, slave := range db.SlaveDatabases {
		driverFunc := drivers[slave.Scheme]
		if driverFunc == nil {
			return nil, fmt.Errorf("%w: %s", ErrUnsupportedDriver, slave.Scheme)
		}
		d = append(d, driverFunc(DriverConfig{
			DatabaseURL:         slave,
			MigrationsTableName: db.MigrationsTableName,
			Log:                 db.Log,
		}))
	}

	return d, nil
}

// GetAllDrivers initializes the appropriate main driver and includes slaves
func (db *DB) GetAllDrivers() ([]Driver, error) {
	drv, err := db.GetDriver()
	if err != nil {
		return nil, err
	}

	slavesDrv, err := db.GetSlaveDrivers()
	if err != nil {
		return nil, err
	}

	d := []Driver{drv}
	d = append(d, slavesDrv...)

	return d, nil
}

// Wait blocks until the database server is available. It does not verify that
// the specified database exists, only that the host is ready to accept connections.
func (db *DB) Wait() error {
	drv, err := db.GetDriver()
	if err != nil {
		return err
	}

	return db.wait([]Driver{drv})
}

func (db *DB) wait(d []Driver) error {
	for _, drv := range d {
		successful := false
		// attempt connection to database server
		err := drv.Ping()
		if err == nil {
			// connection successful
			successful = true
		}

		if !successful {
			fmt.Fprint(db.Log, "Waiting for database")
			for i := 0 * time.Second; i < db.WaitTimeout; i += db.WaitInterval {
				fmt.Fprint(db.Log, ".")
				time.Sleep(db.WaitInterval)

				// attempt connection to database server
				err = drv.Ping()
				if err == nil {
					// connection successful
					fmt.Fprint(db.Log, "\n")
					successful = true
				}
			}
		}

		// if we find outselves here, we could not connect within the timeout
		if !successful {
			fmt.Fprint(db.Log, "\n")
			return fmt.Errorf("%w: %s", ErrCantConnect, err)
		}
	}
	return nil
}

// CreateAndMigrate creates the database (if necessary) and runs migrations
func (db *DB) CreateAndMigrate() error {
	drv, err := db.GetDriver()
	if err != nil {
		return err
	}

	slavesDrv, err := db.GetSlaveDrivers()
	if err != nil {
		return err
	}

	if db.WaitBefore {
		d := []Driver{drv}
		d = append(d, slavesDrv...)
		if err := db.wait(d); err != nil {
			return err
		}
	}

	// create database if it does not already exist
	// skip this step if we cannot determine status
	// (e.g. user does not have list database permission)
	exists, err := drv.DatabaseExists()
	if err == nil && !exists {
		if err := drv.CreateDatabase(); err != nil {
			return err
		}
	}

	// create slave databases if they don't exist
	for _, slaveDrv := range slavesDrv {
		exists, err := slaveDrv.DatabaseExists()
		if err == nil && !exists {
			if err := slaveDrv.CreateDatabase(); err != nil {
				return err
			}
		}
	}

	// migrate
	return db.migrate(drv, slavesDrv)
}

// Create creates the current database and any slaves
func (db *DB) Create() error {
	drv, err := db.GetDriver()
	if err != nil {
		return err
	}

	slavesDrv, err := db.GetSlaveDrivers()
	if err != nil {
		return err
	}

	drvs := []Driver{drv}
	drvs = append(drvs, slavesDrv...)

	if db.WaitBefore {
		if err := db.wait(drvs); err != nil {
			return err
		}
	}

	for _, d := range drvs {
		if err := d.CreateDatabase(); err != nil {
			return err
		}
	}
	return nil
}

// Drop drops the current database (if it exists) and any slaves
func (db *DB) Drop() error {
	drvs, err := db.GetAllDrivers()
	if err != nil {
		return err
	}

	slavesDrv, err := db.GetSlaveDrivers()
	if err != nil {
		return err
	}

	drvs = append(drvs, slavesDrv...)

	if db.WaitBefore {
		if err := db.wait(drvs); err != nil {
			return err
		}
	}

	for _, drv := range drvs {
		if err := drv.DropDatabase(); err != nil {
			return err
		}
	}
	return nil
}

// DumpSchema writes the current database schema to a file
func (db *DB) DumpSchema() error {
	drv, err := db.GetDriver()
	if err != nil {
		return err
	}

	return db.dumpSchema(drv)
}

func (db *DB) dumpSchema(drv Driver) error {
	if db.WaitBefore {
		err := db.wait([]Driver{drv})
		if err != nil {
			return err
		}
	}

	sqlDB, err := db.openDatabaseForMigration(drv)
	if err != nil {
		return err
	}
	defer dbutil.MustClose(sqlDB)

	schema, err := drv.DumpSchema(sqlDB)
	if err != nil {
		return err
	}

	fmt.Fprintf(db.Log, "Writing: %s\n", db.SchemaFile)

	// ensure schema directory exists
	if err = ensureDir(filepath.Dir(db.SchemaFile)); err != nil {
		return err
	}

	// write schema to file
	return os.WriteFile(db.SchemaFile, schema, 0o644)
}

// ensureDir creates a directory if it does not already exist
func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("%w `%s`", ErrCreateDirectory, dir)
	}

	return nil
}

const migrationTemplate = "-- migrate:up\n\n\n-- migrate:down\n\n"

// NewMigration creates a new migration file
func (db *DB) NewMigration(name string) error {
	// new migration name
	timestamp := time.Now().UTC().Format("20060102150405")
	if name == "" {
		return ErrNoMigrationName
	}
	name = fmt.Sprintf("%s_%s.sql", timestamp, name)

	// create migrations dir if missing
	if err := ensureDir(db.MigrationsDir); err != nil {
		return err
	}

	// check file does not already exist
	path := filepath.Join(db.MigrationsDir, name)
	fmt.Fprintf(db.Log, "Creating migration: %s\n", path)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return ErrMigrationAlreadyExist
	}

	// write new migration
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	defer dbutil.MustClose(file)
	_, err = file.WriteString(migrationTemplate)
	return err
}

func doTransaction(sqlDB *sql.DB, txFunc func(dbutil.Transaction) error) error {
	tx, err := sqlDB.Begin()
	if err != nil {
		return err
	}

	if err := txFunc(tx); err != nil {
		if err1 := tx.Rollback(); err1 != nil {
			return err1
		}

		return err
	}

	return tx.Commit()
}

func (db *DB) openDatabaseForMigration(drv Driver) (*sql.DB, error) {
	sqlDB, err := drv.Open()
	if err != nil {
		return nil, err
	}

	if err := drv.CreateMigrationsTable(sqlDB); err != nil {
		dbutil.MustClose(sqlDB)
		return nil, err
	}

	return sqlDB, nil
}

// Migrate migrates database to the latest version
func (db *DB) Migrate() error {
	drv, err := db.GetDriver()
	if err != nil {
		return err
	}

	slavesDrv, err := db.GetSlaveDrivers()
	if err != nil {
		return err
	}

	return db.migrate(drv, slavesDrv)
}

func (db *DB) migrate(drv Driver, slavesDrv []Driver) error {
	files, err := findMigrationFiles(db.MigrationsDir, migrationFileRegexp)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return ErrNoMigrationFiles
	}

	if db.WaitBefore {
		err := db.wait([]Driver{drv})
		if err != nil {
			return err
		}
		err = db.wait(slavesDrv)
		if err != nil {
			return err
		}
	}

	sqlDB, err := db.openDatabaseForMigration(drv)
	if err != nil {
		return err
	}
	defer dbutil.MustClose(sqlDB)

	applied, err := drv.SelectMigrations(sqlDB, -1)
	if err != nil {
		return err
	}

	// Open slaves and
	// Fetch applied to Slaves, indexed the same
	appliedSlaves := make([]map[string]bool, len(slavesDrv))
	slaveSQLDbs := make([]*sql.DB, len(slavesDrv))
	if len(slavesDrv) > 0 {
		for idx, slaveDrv := range slavesDrv {
			slaveSQLDbs[idx], err = db.openDatabaseForMigration(slaveDrv)
			if err != nil {
				return err
			}
			defer dbutil.MustClose(slaveSQLDbs[idx])

			appliedSlaves[idx], err = slaveDrv.SelectMigrations(slaveSQLDbs[idx], -1)
			if err != nil {
				return err
			}
		}
	}

	for _, filename := range files {
		ver := migrationVersion(filename)
		applyToMaster := false
		applyToSlaves := make([]int, 0)

		if ok := applied[ver]; !ok {
			// migration already applied to master
			applyToMaster = true
		}

		if len(slavesDrv) > 0 {
			for idx := range slavesDrv {
				if ok := appliedSlaves[idx][ver]; !ok {
					// This slave doesn't have the migration
					applyToSlaves = append(applyToSlaves, idx)
				}
			}
		}

		if !applyToMaster && len(applyToSlaves) == 0 {
			continue
		}

		fmt.Fprintf(db.Log, "Applying: %s (master: %v, slaves: %d)\n", filename, applyToMaster, len(applyToSlaves))

		up, _, upSlave, _, err := parseMigration(filepath.Join(db.MigrationsDir, filename))
		if err != nil {
			return err
		}

		getExecMigration := func(d Driver, m Migration) func(tx dbutil.Transaction) error {
			return func(tx dbutil.Transaction) error {
				// run actual migration
				result, err := tx.Exec(m.Contents)
				if err != nil {
					return err
				} else if db.Verbose {
					db.printVerbose(result)
				}

				// record migration
				return d.InsertMigration(tx, ver)
			}
		}

		// Apply to Slaves first and fail early
		if len(slavesDrv) > 0 {
			for idx, slaveDrv := range slavesDrv {
				upSlaveTx := getExecMigration(slaveDrv, upSlave)
				if upSlave.Options.Transaction() {
					// begin transaction
					err = doTransaction(slaveSQLDbs[idx], upSlaveTx)
				} else {
					// run outside of transaction
					err = upSlaveTx(slaveSQLDbs[idx])
				}
				if err != nil {
					fmt.Printf("- Failed on slave #%d: %s", idx+1, err.Error())
					return err
				}
				fmt.Printf("- Applied to slave #%d\n", idx+1)
			}
		}

		if applyToMaster {
			upTx := getExecMigration(drv, up)
			if up.Options.Transaction() {
				// begin transaction
				err = doTransaction(sqlDB, upTx)
			} else {
				// run outside of transaction
				err = upTx(sqlDB)
			}
			if err != nil {
				fmt.Printf("- Failed on master: %s", err.Error())
				return err
			}
			fmt.Println("- Applied to master")
		}
	}

	// automatically update schema file, silence errors
	if db.AutoDumpSchema {
		_ = db.dumpSchema(drv)
	}

	return nil
}

func (db *DB) printVerbose(result sql.Result) {
	lastInsertID, err := result.LastInsertId()
	if err == nil {
		fmt.Fprintf(db.Log, "Last insert ID: %d\n", lastInsertID)
	}
	rowsAffected, err := result.RowsAffected()
	if err == nil {
		fmt.Fprintf(db.Log, "Rows affected: %d\n", rowsAffected)
	}
}

func findMigrationFiles(dir string, re *regexp.Regexp) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("%w `%s`", ErrMigrationDirNotFound, dir)
	}

	matches := []string{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		if !re.MatchString(name) {
			continue
		}

		matches = append(matches, name)
	}

	sort.Strings(matches)

	return matches, nil
}

func findMigrationFile(dir string, ver string) (string, error) {
	if ver == "" {
		panic("migration version is required")
	}

	ver = regexp.QuoteMeta(ver)
	re := regexp.MustCompile(fmt.Sprintf(`^%s.*\.sql$`, ver))

	files, err := findMigrationFiles(dir, re)
	if err != nil {
		return "", err
	}

	if len(files) == 0 {
		return "", fmt.Errorf("%w: %s*.sql", ErrMigrationNotFound, ver)
	}

	return files[0], nil
}

func migrationVersion(filename string) string {
	return regexp.MustCompile(`^\d+`).FindString(filename)
}

// Rollback rolls back the most recent migration
func (db *DB) Rollback() error {
	drv, err := db.GetDriver()
	if err != nil {
		return err
	}

	slavesDrv, err := db.GetSlaveDrivers()
	if err != nil {
		return err
	}

	if db.WaitBefore {
		err := db.wait([]Driver{drv})
		if err != nil {
			return err
		}
		err = db.wait(slavesDrv)
		if err != nil {
			return err
		}
	}

	sqlDB, err := db.openDatabaseForMigration(drv)
	if err != nil {
		return err
	}
	defer dbutil.MustClose(sqlDB)

	applied, err := drv.SelectMigrations(sqlDB, 1)
	if err != nil {
		return err
	}

	// grab most recent applied migration (applied has len=1)
	latest := ""
	for ver := range applied {
		latest = ver
	}
	if latest == "" {
		return ErrNoRollback
	}

	filename, err := findMigrationFile(db.MigrationsDir, latest)
	if err != nil {
		return err
	}

	// Open slaves and
	// Fetch applied to Slaves, indexed the same
	appliedSlaves := make([]map[string]bool, len(slavesDrv))
	slaveSQLDbs := make([]*sql.DB, len(slavesDrv))
	if len(slavesDrv) > 0 {
		for idx, slaveDrv := range slavesDrv {
			slaveSQLDbs[idx], err = db.openDatabaseForMigration(slaveDrv)
			if err != nil {
				return err
			}
			defer dbutil.MustClose(slaveSQLDbs[idx])

			appliedSlaves[idx], err = slaveDrv.SelectMigrations(slaveSQLDbs[idx], 1)
			if err != nil {
				return err
			}
			// Ensure slave has same last version as master
			for slaveVer := range appliedSlaves[idx] {
				if slaveVer != latest {
					return ErrNoRollbackSlaves
				}
			}
		}
	}

	fmt.Fprintf(db.Log, "Rolling back: %s\n", filename)

	_, down, _, downSlave, err := parseMigration(filepath.Join(db.MigrationsDir, filename))
	if err != nil {
		return err
	}

	getExecMigration := func(d Driver, m Migration) func(tx dbutil.Transaction) error {
		return func(tx dbutil.Transaction) error {
			// rollback migration
			result, err := tx.Exec(m.Contents)
			if err != nil {
				return err
			} else if db.Verbose {
				db.printVerbose(result)
			}

			// remove migration record
			return d.DeleteMigration(tx, latest)
		}
	}

	downTx := getExecMigration(drv, down)
	if down.Options.Transaction() {
		// begin transaction
		err = doTransaction(sqlDB, downTx)
	} else {
		// run outside of transaction
		err = downTx(sqlDB)
	}
	if err != nil {
		fmt.Printf("- Failed on master: %s", err.Error())
		return err
	}
	fmt.Println("- Applied to master")

	// Do slaves next
	if len(slavesDrv) > 0 {
		for idx, slaveDrv := range slavesDrv {
			downSlaveTx := getExecMigration(slaveDrv, downSlave)
			if downSlave.Options.Transaction() {
				// begin transaction
				err = doTransaction(slaveSQLDbs[idx], downSlaveTx)
			} else {
				// run outside of transaction
				err = downSlaveTx(slaveSQLDbs[idx])
			}
			if err != nil {
				fmt.Printf("- Failed on slave #%d: %s", idx+1, err.Error())
				return err
			}
			fmt.Printf("- Applied to slave #%d\n", idx+1)
		}
	}

	// automatically update schema file, silence errors
	if db.AutoDumpSchema {
		_ = db.dumpSchema(drv)
	}

	return nil
}

// Status shows the status of all migrations
func (db *DB) Status(quiet bool) (int, error) {
	drv, err := db.GetDriver()
	if err != nil {
		return -1, err
	}

	results, err := db.CheckMigrationsStatus(drv)
	if err != nil {
		return -1, err
	}

	var totalApplied int
	var line string

	for _, res := range results {
		if res.Applied {
			line = fmt.Sprintf("[X] %s", res.Filename)
			totalApplied++
		} else {
			line = fmt.Sprintf("[ ] %s", res.Filename)
		}
		if !quiet {
			fmt.Fprintln(db.Log, line)
		}
	}

	totalPending := len(results) - totalApplied
	if !quiet {
		fmt.Fprintln(db.Log)
		fmt.Fprintf(db.Log, "Applied: %d\n", totalApplied)
		fmt.Fprintf(db.Log, "Pending: %d\n", totalPending)
	}

	return totalPending, nil
}

// CheckMigrationsStatus returns the status of all available mgirations
func (db *DB) CheckMigrationsStatus(drv Driver) ([]StatusResult, error) {
	files, err := findMigrationFiles(db.MigrationsDir, migrationFileRegexp)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, ErrNoMigrationFiles
	}

	sqlDB, err := drv.Open()
	if err != nil {
		return nil, err
	}
	defer dbutil.MustClose(sqlDB)

	applied := map[string]bool{}

	migrationsTableExists, err := drv.MigrationsTableExists(sqlDB)
	if err != nil {
		return nil, err
	}

	if migrationsTableExists {
		applied, err = drv.SelectMigrations(sqlDB, -1)
		if err != nil {
			return nil, err
		}
	}

	var results []StatusResult

	for _, filename := range files {
		ver := migrationVersion(filename)
		res := StatusResult{Filename: filename}
		if ok := applied[ver]; ok {
			res.Applied = true
		} else {
			res.Applied = false
		}

		results = append(results, res)
	}

	return results, nil
}
