package defer_cleanup_chain

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
)

// Tx models the transaction lifecycle we need for this kata.
// Keeping this small makes it easy to provide fake implementations in tests.
type Tx interface {
	Commit() error
	Rollback() error
}

// Rows models a streaming result set.
// Values returns the current row payload after Next() moved to a valid item.
type Rows interface {
	Next() bool
	Values() (id int, payload string, err error)
	Err() error
	Close() error
}

// DB is the minimal database contract needed by BackupDatabase.
// The implementation can be real SQL, in-memory, or fully mocked in tests.
type DB interface {
	Begin(ctx context.Context) (Tx, error)
	QueryBackupRows(ctx context.Context, tx Tx) (Rows, error)
	Close() error
}

// Connector abstracts DB creation so tests can inject controlled failures.
type Connector func(ctx context.Context, dbURL string) (DB, error)

// defaultConnector is intentionally unconfigured.
// In real code this would be wired to an actual driver in init/bootstrap code.
var defaultConnector Connector = func(ctx context.Context, dbURL string) (DB, error) {
	return nil, errors.New("no database connector configured")
}

// BackupDatabase performs the backup flow while preserving both operation and
// cleanup errors. It uses a named return error so deferred cleanups can append
// their own failures via errors.Join.
func BackupDatabase(ctx context.Context, dbURL, filename string) (err error) {
	return backupDatabase(ctx, dbURL, filename, defaultConnector, os.Create)
}

// backupDatabase is split from BackupDatabase to keep the public API clean while
// allowing tests to inject open/connect behavior and force edge cases.
func backupDatabase(
	ctx context.Context,
	dbURL, filename string,
	connect Connector,
	openFile func(name string) (*os.File, error),
) (err error) {
	// Acquire #1: output file.
	// Defer is registered immediately so every return path closes the file.
	file, err := openFile(filename)
	if err != nil {
		return fmt.Errorf("open output file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close output file: %w", closeErr))
		}
	}()

	// Wrap the file with a buffered writer. This is another acquired resource:
	// data can remain in memory until Flush runs, so we must defer Flush as well.
	writer := bufio.NewWriter(file)
	defer func() {
		if flushErr := writer.Flush(); flushErr != nil {
			err = errors.Join(err, fmt.Errorf("flush writer: %w", flushErr))
		}
	}()

	// Acquire #2: DB connection.
	db, err := connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("connect DB: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close DB connection: %w", closeErr))
		}
	}()

	// Acquire #3: transaction.
	// We avoid manual rollback branches by using a committed flag consumed by defer.
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("rollback transaction: %w", rollbackErr))
		}
	}()

	// Acquire #4: row stream (logical cursor/network resource).
	rows, err := db.QueryBackupRows(ctx, tx)
	if err != nil {
		return fmt.Errorf("query backup rows: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close rows: %w", closeErr))
		}
	}()

	// Stream each row to the backup file.
	// We intentionally do not defer anything inside this loop.
	for rows.Next() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("backup canceled: %w", ctxErr)
		}

		id, payload, rowErr := rows.Values()
		if rowErr != nil {
			return fmt.Errorf("read row values: %w", rowErr)
		}

		if _, writeErr := fmt.Fprintf(writer, "%d,%s\n", id, payload); writeErr != nil {
			return fmt.Errorf("write backup row: %w", writeErr)
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("iterate backup rows: %w", rowsErr)
	}

	// Commit is the success boundary.
	// If commit fails, committed remains false and deferred rollback still runs.
	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("commit transaction: %w", commitErr)
	}
	committed = true

	return nil
}
