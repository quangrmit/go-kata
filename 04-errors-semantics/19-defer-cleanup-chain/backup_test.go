package defer_cleanup_chain

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRow struct {
	id      int
	payload string
	err     error
}

type fakeRows struct {
	data     []fakeRow
	nextIdx  int
	iterErr  error
	closeErr error
	closed   bool
}

func (r *fakeRows) Next() bool {
	if r.nextIdx >= len(r.data) {
		return false
	}
	r.nextIdx++
	return true
}

func (r *fakeRows) Values() (int, string, error) {
	if r.nextIdx == 0 || r.nextIdx > len(r.data) {
		return 0, "", errors.New("values called without current row")
	}
	row := r.data[r.nextIdx-1]
	if row.err != nil {
		return 0, "", row.err
	}
	return row.id, row.payload, nil
}

func (r *fakeRows) Err() error {
	return r.iterErr
}

func (r *fakeRows) Close() error {
	r.closed = true
	return r.closeErr
}

type fakeTx struct {
	commitErr      error
	rollbackErr    error
	commitCalled   int
	rollbackCalled int
}

func (tx *fakeTx) Commit() error {
	tx.commitCalled++
	return tx.commitErr
}

func (tx *fakeTx) Rollback() error {
	tx.rollbackCalled++
	return tx.rollbackErr
}

type fakeDB struct {
	tx          Tx
	rows        Rows
	beginErr    error
	queryErr    error
	closeErr    error
	beginCalled int
	queryCalled int
	closeCalled int
}

func (db *fakeDB) Begin(context.Context) (Tx, error) {
	db.beginCalled++
	if db.beginErr != nil {
		return nil, db.beginErr
	}
	return db.tx, nil
}

func (db *fakeDB) QueryBackupRows(context.Context, Tx) (Rows, error) {
	db.queryCalled++
	if db.queryErr != nil {
		return nil, db.queryErr
	}
	return db.rows, nil
}

func (db *fakeDB) Close() error {
	db.closeCalled++
	return db.closeErr
}

func TestBackupDatabase_Matrix(t *testing.T) {
	type tc struct {
		name string

		connectErr      error
		openErr         error
		beginErr        error
		queryErr        error
		rows            []fakeRow
		iterErr         error
		rowsCloseErr    error
		commitErr       error
		rollbackErr     error
		dbCloseErr      error
		cancelCtx       bool
		openReadOnly    bool
		largePayloadRow bool

		wantErr           bool
		wantErrContains   []string
		wantErrIs         []error
		wantCommitCalls   int
		wantRollbackCalls int
		wantDBCloseCalls  int
		wantRowsClosed    bool
		wantOutput        string
	}

	errConnectBoom := errors.New("connect boom")
	errOpenBoom := errors.New("open boom")
	errBeginBoom := errors.New("begin boom")
	errQueryBoom := errors.New("query boom")
	errRollbackBoom := errors.New("rollback boom")
	errDBCloseBoom := errors.New("db close boom")
	errValuesBoom := errors.New("values boom")
	errIterateBoom := errors.New("iterate boom")
	errCommitBoom := errors.New("commit boom")

	tests := []tc{
		{
			name:              "success commits and writes all rows",
			rows:              []fakeRow{{id: 1, payload: "alpha"}, {id: 2, payload: "beta"}},
			wantCommitCalls:   1,
			wantRollbackCalls: 0,
			wantDBCloseCalls:  1,
			wantRowsClosed:    true,
			wantOutput:        "1,alpha\n2,beta\n",
		},
		{
			name:              "connect failure returns wrapped error",
			connectErr:        errConnectBoom,
			wantErr:           true,
			wantErrContains:   []string{"connect DB"},
			wantErrIs:         []error{errConnectBoom},
			wantCommitCalls:   0,
			wantRollbackCalls: 0,
			wantDBCloseCalls:  0,
			wantRowsClosed:    false,
		},
		{
			name:              "begin failure keeps cleanup error with join",
			beginErr:          errBeginBoom,
			dbCloseErr:        errDBCloseBoom,
			wantErr:           true,
			wantErrContains:   []string{"begin transaction", "close DB connection"},
			wantErrIs:         []error{errBeginBoom, errDBCloseBoom},
			wantCommitCalls:   0,
			wantRollbackCalls: 0,
			wantDBCloseCalls:  1,
			wantRowsClosed:    false,
		},
		{
			name:              "query failure triggers rollback and preserves cleanup failures",
			queryErr:          errQueryBoom,
			rollbackErr:       errRollbackBoom,
			dbCloseErr:        errDBCloseBoom,
			wantErr:           true,
			wantErrContains:   []string{"query backup rows", "rollback transaction", "close DB connection"},
			wantErrIs:         []error{errQueryBoom, errRollbackBoom, errDBCloseBoom},
			wantCommitCalls:   0,
			wantRollbackCalls: 1,
			wantDBCloseCalls:  1,
			wantRowsClosed:    false,
		},
		{
			name:              "row values failure stops stream and rolls back",
			rows:              []fakeRow{{id: 1, payload: "ok"}, {err: errValuesBoom}},
			wantErr:           true,
			wantErrContains:   []string{"read row values"},
			wantErrIs:         []error{errValuesBoom},
			wantCommitCalls:   0,
			wantRollbackCalls: 1,
			wantDBCloseCalls:  1,
			wantRowsClosed:    true,
		},
		{
			name:              "rows err after iteration rolls back",
			rows:              []fakeRow{{id: 1, payload: "ok"}},
			iterErr:           errIterateBoom,
			wantErr:           true,
			wantErrContains:   []string{"iterate backup rows"},
			wantErrIs:         []error{errIterateBoom},
			wantCommitCalls:   0,
			wantRollbackCalls: 1,
			wantDBCloseCalls:  1,
			wantRowsClosed:    true,
		},
		{
			name:              "commit failure keeps rollback and close errors",
			rows:              []fakeRow{{id: 1, payload: "ok"}},
			commitErr:         errCommitBoom,
			rollbackErr:       errRollbackBoom,
			dbCloseErr:        errDBCloseBoom,
			wantErr:           true,
			wantErrContains:   []string{"commit transaction", "rollback transaction", "close DB connection"},
			wantErrIs:         []error{errCommitBoom, errRollbackBoom, errDBCloseBoom},
			wantCommitCalls:   1,
			wantRollbackCalls: 1,
			wantDBCloseCalls:  1,
			wantRowsClosed:    true,
		},
		{
			name:              "context cancellation in loop returns cancellation error",
			rows:              []fakeRow{{id: 1, payload: "ok"}},
			cancelCtx:         true,
			wantErr:           true,
			wantErrContains:   []string{"backup canceled", context.Canceled.Error()},
			wantErrIs:         []error{context.Canceled},
			wantCommitCalls:   0,
			wantRollbackCalls: 1,
			wantDBCloseCalls:  1,
			wantRowsClosed:    true,
		},
		{
			name:              "flush failure after successful work is returned",
			rows:              []fakeRow{{id: 1, payload: "ok"}},
			openReadOnly:      true,
			wantErr:           true,
			wantErrContains:   []string{"flush writer"},
			wantCommitCalls:   1,
			wantRollbackCalls: 0,
			wantDBCloseCalls:  1,
			wantRowsClosed:    true,
		},
		{
			name:              "write failure in loop is returned",
			rows:              []fakeRow{{id: 1, payload: "ok"}},
			openReadOnly:      true,
			largePayloadRow:   true,
			wantErr:           true,
			wantErrContains:   []string{"write backup row"},
			wantCommitCalls:   0,
			wantRollbackCalls: 1,
			wantDBCloseCalls:  1,
			wantRowsClosed:    true,
		},
		{
			name:              "open file failure short-circuits immediately",
			openErr:           errOpenBoom,
			wantErr:           true,
			wantErrContains:   []string{"open output file"},
			wantErrIs:         []error{errOpenBoom},
			wantCommitCalls:   0,
			wantRollbackCalls: 0,
			wantDBCloseCalls:  0,
			wantRowsClosed:    false,
		},
		{
			name:              "rows close failure is preserved",
			rows:              []fakeRow{{id: 1, payload: "ok"}},
			rowsCloseErr:      errors.New("rows close boom"),
			wantErr:           true,
			wantErrContains:   []string{"close rows"},
			wantCommitCalls:   1,
			wantRollbackCalls: 0,
			wantDBCloseCalls:  1,
			wantRowsClosed:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			filename := filepath.Join(dir, "backup.csv")

			if tt.openReadOnly {
				require.NoError(t, os.WriteFile(filename, nil, 0o644))
			}

			rowsData := tt.rows
			if tt.largePayloadRow {
				rowsData = []fakeRow{{id: 1, payload: strings.Repeat("x", 9000)}}
			}

			tx := &fakeTx{commitErr: tt.commitErr, rollbackErr: tt.rollbackErr}
			rows := &fakeRows{data: rowsData, iterErr: tt.iterErr, closeErr: tt.rowsCloseErr}
			db := &fakeDB{
				tx:       tx,
				rows:     rows,
				beginErr: tt.beginErr,
				queryErr: tt.queryErr,
				closeErr: tt.dbCloseErr,
			}

			connect := func(context.Context, string) (DB, error) {
				if tt.connectErr != nil {
					return nil, tt.connectErr
				}
				return db, nil
			}

			openFile := os.Create
			if tt.openErr != nil {
				openFile = func(string) (*os.File, error) {
					return nil, tt.openErr
				}
			} else if tt.openReadOnly {
				openFile = func(name string) (*os.File, error) {
					return os.Open(name)
				}
			}

			ctx := context.Background()
			if tt.cancelCtx {
				ctxWithCancel, cancel := context.WithCancel(context.Background())
				cancel()
				ctx = ctxWithCancel
			}

			err := backupDatabase(ctx, "db://test", filename, connect, openFile)

			if tt.wantErr {
				require.Error(t, err)
				for _, substr := range tt.wantErrContains {
					assert.ErrorContains(t, err, substr)
				}
				for _, targetErr := range tt.wantErrIs {
					assert.ErrorIs(t, err, targetErr)
				}
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCommitCalls, tx.commitCalled)
			assert.Equal(t, tt.wantRollbackCalls, tx.rollbackCalled)
			assert.Equal(t, tt.wantDBCloseCalls, db.closeCalled)
			assert.Equal(t, tt.wantRowsClosed, rows.closed)

			if tt.wantOutput != "" {
				got, readErr := os.ReadFile(filename)
				require.NoError(t, readErr)
				assert.Equal(t, tt.wantOutput, string(got))
			}
		})
	}
}

func TestBackupDatabase_SelfCorrection_BeginFails_ClosesFileAndDB(t *testing.T) {
	errBegin := errors.New("begin failed")

	dir := t.TempDir()
	filename := filepath.Join(dir, "backup.csv")

	var openedFile *os.File
	openFile := func(name string) (*os.File, error) {
		f, err := os.Create(name)
		if err != nil {
			return nil, err
		}
		openedFile = f
		return f, nil
	}

	db := &fakeDB{beginErr: errBegin}
	connect := func(context.Context, string) (DB, error) {
		return db, nil
	}

	err := backupDatabase(context.Background(), "db://test", filename, connect, openFile)
	require.Error(t, err)
	assert.ErrorContains(t, err, "begin transaction")
	assert.ErrorIs(t, err, errBegin)
	assert.Equal(t, 1, db.closeCalled, "db close must run even when Begin fails")
	require.NotNil(t, openedFile)
	assert.ErrorIs(t, openedFile.Close(), os.ErrClosed, "file must already be closed by defer")
}

func TestBackupDatabase_SelfCorrection_CommitFailsAndFileCloseFails_BothErrorsReturned(t *testing.T) {
	errCommit := errors.New("commit failed")

	badFile := os.NewFile(^uintptr(0), "invalid-fd")
	openFile := func(string) (*os.File, error) {
		return badFile, nil
	}

	rows := &fakeRows{}
	tx := &fakeTx{commitErr: errCommit}
	db := &fakeDB{tx: tx, rows: rows}
	connect := func(context.Context, string) (DB, error) {
		return db, nil
	}

	err := backupDatabase(context.Background(), "db://test", "ignored.csv", connect, openFile)
	require.Error(t, err)
	assert.ErrorContains(t, err, "commit transaction")
	assert.ErrorContains(t, err, "close output file")
	assert.ErrorIs(t, err, errCommit)
	assert.Equal(t, 1, tx.commitCalled)
	assert.Equal(t, 1, tx.rollbackCalled, "rollback must run after failed commit")
}

func TestBackupDatabase_SelfCorrection_NoFDLeak_1000Runs(t *testing.T) {
	before, err := countOpenFDs()
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		dir := t.TempDir()
		filename := filepath.Join(dir, "backup.csv")

		rows := &fakeRows{}
		tx := &fakeTx{}
		db := &fakeDB{tx: tx, rows: rows}
		connect := func(context.Context, string) (DB, error) {
			return db, nil
		}

		err := backupDatabase(context.Background(), "db://test", filename, connect, os.Create)
		require.NoError(t, err)
	}

	runtime.GC()
	after, err := countOpenFDs()
	require.NoError(t, err)

	// Allow a tiny delta for runtime/testing internals while asserting no growth trend.
	assert.LessOrEqual(t, after, before+3, "open file descriptors should not grow after 1000 runs")
}

func TestBackupDatabase_PublicWrapper_Success(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "backup.csv")

	oldConnector := defaultConnector
	t.Cleanup(func() {
		defaultConnector = oldConnector
	})

	tx := &fakeTx{}
	rows := &fakeRows{data: []fakeRow{{id: 7, payload: "public"}}}
	db := &fakeDB{tx: tx, rows: rows}

	defaultConnector = func(context.Context, string) (DB, error) {
		return db, nil
	}

	err := BackupDatabase(context.Background(), "db://public", filename)
	require.NoError(t, err)
	assert.Equal(t, 1, tx.commitCalled)
	assert.Equal(t, 0, tx.rollbackCalled)
	assert.Equal(t, 1, db.closeCalled)
	assert.True(t, rows.closed)

	got, readErr := os.ReadFile(filename)
	require.NoError(t, readErr)
	assert.Equal(t, "7,public\n", string(got))
}

func TestBackupDatabase_PublicWrapper_ConnectorError(t *testing.T) {
	errConnect := errors.New("public connector fail")

	oldConnector := defaultConnector
	t.Cleanup(func() {
		defaultConnector = oldConnector
	})

	defaultConnector = func(context.Context, string) (DB, error) {
		return nil, errConnect
	}

	filename := filepath.Join(t.TempDir(), "backup.csv")
	err := BackupDatabase(context.Background(), "db://public", filename)
	require.Error(t, err)
	assert.ErrorContains(t, err, "connect DB")
	assert.ErrorIs(t, err, errConnect)
}

func TestBackupDatabase_PublicWrapper_DefaultConnectorUnconfigured(t *testing.T) {
	oldConnector := defaultConnector
	t.Cleanup(func() {
		defaultConnector = oldConnector
	})

	defaultConnector = oldConnector

	filename := filepath.Join(t.TempDir(), "backup.csv")
	err := BackupDatabase(context.Background(), "db://public", filename)
	require.Error(t, err)
	assert.ErrorContains(t, err, "connect DB")
	assert.ErrorContains(t, err, "no database connector configured")
}

func countOpenFDs() (int, error) {
	var lastErr error

	for _, path := range []string{"/proc/self/fd", "/dev/fd"} {
		count, err := countNumericDirEntries(path)
		if err == nil {
			return count, nil
		}
		lastErr = err
	}

	if lastErr == nil {
		lastErr = errors.New("no fd directory available")
	}

	return 0, lastErr
}

func countNumericDirEntries(path string) (int, error) {
	dir, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, name := range names {
		if _, convErr := strconv.Atoi(name); convErr == nil {
			count++
		}
	}

	return count, nil
}
