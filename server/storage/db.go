package storage

import (
	"database/sql"
	"embed"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrations embed.FS

// OpenDB opens the storage metadata database at dbPath, runs migrations
// to the requested version, and returns the open connection.
//
// Pattern mirrors server/gcas/db.go to keep the project consistent.
func OpenDB(dbPath string, version uint) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, err
	}

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=10000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}

	db.SetMaxOpenConns(1)

	driver, err := sqlite.WithInstance(db, &sqlite.Config{})
	if err != nil {
		db.Close()
		return nil, err
	}

	migrationFs, err := iofs.New(migrations, "migrations")
	if err != nil {
		db.Close()
		return nil, err
	}

	migrator, err := migrate.NewWithInstance(
		"iofs",
		migrationFs,
		"sqlite",
		driver,
	)
	if err != nil {
		db.Close()
		return nil, err
	}

	if err = migrator.Migrate(version); err != nil && err != migrate.ErrNoChange {
		return nil, err
	}

	return db, nil
}
