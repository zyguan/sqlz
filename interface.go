package sqlz

import (
	"context"
	"database/sql"
)

type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

type PreparerContext interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type PreparedExecer interface {
	Exec(args ...interface{}) (sql.Result, error)
}

type ExecerContext interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type PreparedExecerContext interface {
	ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)
}

type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

type PreparedQueryer interface {
	Query(args ...interface{}) (*sql.Rows, error)
}

type QueryerContext interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type PreparedQueryerContext interface {
	QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error)
}

type ConnPool interface {
	Conn(ctx context.Context) (*sql.Conn, error)
}

type Conn interface {
	Execer
	Queryer
}

type ConnContext interface {
	ExecerContext
	QueryerContext
}

type Stmt interface {
	PreparedExecer
	PreparedQueryer
}

type StmtContext interface {
	PreparedExecerContext
	PreparedQueryerContext
}

type RowIterator interface {
	Close() error
	Columns() ([]string, error)
	ColumnTypes() ([]*sql.ColumnType, error)
	Err() error
	Next() bool
	Scan(...interface{}) error
}
