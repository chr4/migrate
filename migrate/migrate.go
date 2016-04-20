// Package migrate is imported by other Go code.
// It is the entry point to all migration functions.
package migrate

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"

	"github.com/chr4/migrate/driver"
	"github.com/chr4/migrate/file"
	"github.com/chr4/migrate/migrate/direction"
)

// Up applies all available migrations
func Up(url, migrationsPath string) (err error) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	defer d.Close()
	if err != nil {
		return
	}

	// Discarding error, files.ToLastFrom() always returns Files, nil
	applyMigrationFiles, _ := files.ToLastFrom(version)

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			err = d.Migrate(f)
			if err != nil {
				return
			}
		}
	}

	return
}

// Down rolls back all migrations
func Down(url, migrationsPath string) (err error) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		return
	}

	// Discarding error, files.ToLastFrom() always returns Files, nil
	applyMigrationFiles, _ := files.ToFirstFrom(version)

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			err = d.Migrate(f)
			if err != nil {
				break
			}
		}
	}
	return
}

// Redo rolls back the most recently applied migration, then runs it again.
func Redo(url, migrationsPath string) (err error) {
	err = Migrate(url, migrationsPath, -1)
	if err != nil {
		return
	}

	err = Migrate(url, migrationsPath, +1)
	return
}

// Reset runs the down and up migration function
func Reset(url, migrationsPath string) (err error) {
	err = Down(url, migrationsPath)
	if err != nil {
		return
	}
	err = Up(url, migrationsPath)
	return
}

// Migrate applies relative +n/-n migrations
func Migrate(url, migrationsPath string, relativeN int) (err error) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	defer d.Close()

	if err != nil {
		return
	}

	applyMigrationFiles, err := files.From(version, relativeN)
	if err != nil {
		return
	}

	if len(applyMigrationFiles) > 0 && relativeN != 0 {
		for _, f := range applyMigrationFiles {
			err = d.Migrate(f)
			if err != nil {
				return
			}
		}
		return
	}
	return
}

// Version returns the current migration version
func Version(url, migrationsPath string) (version uint64, err error) {
	d, err := driver.New(url)
	if err != nil {
		return 0, err
	}
	return d.Version()
}

// Create creates new migration files on disk
func Create(url, migrationsPath, name string) (*file.MigrationFile, error) {
	d, err := driver.New(url)
	if err != nil {
		return nil, err
	}
	files, err := file.ReadMigrationFiles(migrationsPath, file.FilenameRegex(d.FilenameExtension()))
	if err != nil {
		return nil, err
	}

	version := uint64(0)
	if len(files) > 0 {
		lastFile := files[len(files)-1]
		version = lastFile.Version
	}
	version += 1
	versionStr := strconv.FormatUint(version, 10)

	length := 4 // TODO(mattes) check existing files and try to guess length
	if len(versionStr)%length != 0 {
		versionStr = strings.Repeat("0", length-len(versionStr)%length) + versionStr
	}

	filenamef := "%s_%s.%s.%s"
	name = strings.Replace(name, " ", "_", -1)

	mfile := &file.MigrationFile{
		Version: version,
		UpFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, versionStr, name, "up", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Up,
		},
		DownFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, versionStr, name, "down", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Down,
		},
	}

	if err := ioutil.WriteFile(path.Join(mfile.UpFile.Path, mfile.UpFile.FileName), mfile.UpFile.Content, 0644); err != nil {
		return nil, err
	}
	if err := ioutil.WriteFile(path.Join(mfile.DownFile.Path, mfile.DownFile.FileName), mfile.DownFile.Content, 0644); err != nil {
		return nil, err
	}

	return mfile, nil
}

// initDriverAndReadMigrationFilesAndGetVersion is a small helper
// function that is common to most of the migration funcs
func initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath string) (driver.Driver, *file.MigrationFiles, uint64, error) {
	d, err := driver.New(url)
	if err != nil {
		return nil, nil, 0, err
	}
	files, err := file.ReadMigrationFiles(migrationsPath, file.FilenameRegex(d.FilenameExtension()))
	if err != nil {
		d.Close() // TODO what happens with errors from this func?
		return nil, nil, 0, err
	}
	version, err := d.Version()
	if err != nil {
		d.Close() // TODO what happens with errors from this func?
		return nil, nil, 0, err
	}
	return d, &files, version, nil
}

// interrupts is an internal variable that holds the state of
// interrupt handling
var interrupts = true

// Graceful enables interrupts checking. Once the first ^C is received
// it will finish the currently running migration and abort execution
// of the next migration. If ^C is received twice, it will stop
// execution immediately.
func Graceful() {
	interrupts = true
}

// NonGraceful disables interrupts checking. The first received ^C will
// stop execution immediately.
func NonGraceful() {
	interrupts = false
}

// interrupts returns a signal channel if interrupts checking is
// enabled. nil otherwise.
func handleInterrupts() chan os.Signal {
	if interrupts {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		return c
	}
	return nil
}
