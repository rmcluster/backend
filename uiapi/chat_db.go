package uiapi

import (
	"database/sql"
	"embed"
	"errors"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "modernc.org/sqlite"
)

const chatPragmaString = `PRAGMA journal_mode=WAL;
		PRAGMA synchronous=NORMAL;
		PRAGMA busy_timeout=10000;
		PRAGMA foreign_keys=ON;`

//go:embed chat_migrations/*.sql
var chatMigrations embed.FS

func OpenChatDB(dbPath string, version uint) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec(chatPragmaString); err != nil {
		_ = db.Close()
		return nil, err
	}

	db.SetMaxOpenConns(1)

	driver, err := sqlite.WithInstance(db, &sqlite.Config{})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	migrationFS, err := iofs.New(chatMigrations, "chat_migrations")
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	migrator, err := migrate.NewWithInstance("iofs", migrationFS, "sqlite", driver)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := migrator.Migrate(version); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
