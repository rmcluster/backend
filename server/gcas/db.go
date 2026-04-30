package gcas

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

func OpenDB(dbPath string, version uint) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath+"?pragma=busy_timeout=10000")
	if err != nil {
		return nil, err
	}

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

	if err = migrator.Migrate(version); err != nil {
		return nil, err
	}

	return db, nil
}
