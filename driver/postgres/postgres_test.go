package postgres

import (
	"database/sql"
	"os"
	"testing"

	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
)

// TestMigrate runs some additional tests on Migrate().
// Basic testing is already done in migrate/migrate_test.go
func TestMigrate(t *testing.T) {
	host := os.Getenv("POSTGRES_PORT_5432_TCP_ADDR")
	port := os.Getenv("POSTGRES_PORT_5432_TCP_PORT")
	driverUrl := "postgres://postgres@" + host + ":" + port + "/template1?sslmode=disable"

	// prepare clean database
	connection, err := sql.Open("postgres", driverUrl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(`
				DROP TABLE IF EXISTS yolo;
				DROP TABLE IF EXISTS ` + tableName + `;`); err != nil {
		t.Fatal(err)
	}

	d := &Driver{}
	if err := d.Initialize(driverUrl); err != nil {
		t.Fatal(err)
	}

	files := []file.File{
		{
			Path:      "/foobar",
			FileName:  "001_foobar.up.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
				CREATE TABLE yolo (
					id serial not null primary key
				);
			`),
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.down.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
				DROP TABLE yolo;
			`),
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.up.sql",
			Version:   2,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
				CREATE TABLE error (
					id THIS WILL CAUSE AN ERROR
				)
			`),
		},
	}

	err := d.Migrate(files[0])
	if err != nil {
		t.Fatal(err)
	}

	err = d.Migrate(files[1])
	if err != nil {
		t.Fatal(err)
	}

	err = d.Migrate(files[2])
	if err == nil {
		t.Error("Expected test case to fail")
	}

	err = d.Close()
	if err != nil {
		t.Fatal(err)
	}
}
