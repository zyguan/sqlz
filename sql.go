package sqlz

import (
	"context"
	"database/sql"

	"github.com/zyguan/sqlz/resultset"

	. "github.com/zyguan/just"
)

func Connect(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	for {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		if err := conn.PingContext(ctx); err == nil {
			return conn, nil
		}
		conn.Close()
	}
}

type DB struct {
	*sql.DB
	ctx context.Context
}

func WrapDB(ctx context.Context, db *sql.DB) *DB { return &DB{db, ctx} }

func (db *DB) Connect() (*Conn, error) {
	return db.ConnectContext(db.ctx)
}

func (db *DB) ConnectContext(ctx context.Context) (*Conn, error) {
	conn, err := Connect(ctx, db.DB)
	if err != nil {
		return nil, err
	}
	return WrapConn(ctx, conn), nil
}

func (db *DB) WithConns(n int, do func(conns ...*Conn) error) error {
	conns := make([]*Conn, n)
	ctx, cancel := context.WithCancel(db.ctx)
	defer func() {
		cancel()
		for _, conn := range conns {
			if conn != nil {
				conn.Close()
			}
		}
	}()
	for i := 0; i < n; i++ {
		conn, err := db.ConnectContext(ctx)
		if err != nil {
			return err
		}
		conns[i] = conn
	}
	return do(conns...)
}

func (db *DB) MustFetch(query string, args ...interface{}) *resultset.ResultSet {
	rows := db.MustQuery(query, args...)
	defer rows.Close()
	return Try(resultset.ReadFromRows(rows)).(*resultset.ResultSet)
}

func (db *DB) MustQuery(query string, args ...interface{}) *sql.Rows {
	return Try(db.QueryContext(db.ctx, query, args...)).(*sql.Rows)
}

func (db *DB) MustExec(query string, args ...interface{}) sql.Result {
	return Try(db.ExecContext(db.ctx, query, args...)).(sql.Result)
}

func (db *DB) AsyncFetch(query string, args ...interface{}) <-chan Values {
	future := make(chan Values, 1)
	go func() {
		defer close(future)
		rows, err := db.QueryContext(db.ctx, query, args...)
		if err != nil {
			future <- Pack(nil, err)
			return
		}
		defer rows.Close()
		future <- Pack(resultset.ReadFromRows(rows))
	}()
	return future
}

func (db *DB) AsyncQuery(query string, args ...interface{}) <-chan Values {
	future := make(chan Values, 1)
	go func() {
		defer close(future)
		future <- Pack(db.QueryContext(db.ctx, query, args...))
	}()
	return future
}

func (db *DB) AsyncQueryRow(query string, args ...interface{}) <-chan Values {
	future := make(chan Values, 1)
	go func() {
		defer close(future)
		future <- Pack(db.QueryRowContext(db.ctx, query, args...))
	}()
	return future
}

func (db *DB) AsyncExec(query string, args ...interface{}) <-chan Values {
	future := make(chan Values, 1)
	go func() {
		defer close(future)
		future <- Pack(db.ExecContext(db.ctx, query, args...))
	}()
	return future
}

type Conn struct {
	*sql.Conn
	ctx context.Context
}

func WrapConn(ctx context.Context, conn *sql.Conn) *Conn { return &Conn{conn, ctx} }

func (conn *Conn) MustFetch(query string, args ...interface{}) *resultset.ResultSet {
	rows := conn.MustQuery(query, args...)
	defer rows.Close()
	return Try(resultset.ReadFromRows(rows)).(*resultset.ResultSet)
}

func (conn *Conn) MustQuery(query string, args ...interface{}) *sql.Rows {
	return Try(conn.QueryContext(conn.ctx, query, args...)).(*sql.Rows)
}

func (conn *Conn) MustExec(query string, args ...interface{}) sql.Result {
	return Try(conn.ExecContext(conn.ctx, query, args...)).(sql.Result)
}

func (conn *Conn) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return conn.QueryContext(conn.ctx, query, args...)
}

func (conn *Conn) QueryRow(query string, args ...interface{}) *sql.Row {
	return conn.QueryRowContext(conn.ctx, query, args...)
}

func (conn *Conn) Exec(query string, args ...interface{}) (sql.Result, error) {
	return conn.ExecContext(conn.ctx, query, args...)
}

func (conn *Conn) AsyncFetch(query string, args ...interface{}) <-chan Values {
	future := make(chan Values, 1)
	go func() {
		defer close(future)
		rows, err := conn.QueryContext(conn.ctx, query, args...)
		if err != nil {
			future <- Pack(nil, err)
			return
		}
		defer rows.Close()
		future <- Pack(resultset.ReadFromRows(rows))
	}()
	return future
}

func (conn *Conn) AsyncQuery(query string, args ...interface{}) <-chan Values {
	future := make(chan Values, 1)
	go func() {
		defer close(future)
		future <- Pack(conn.QueryContext(conn.ctx, query, args...))
	}()
	return future
}

func (conn *Conn) AsyncQueryRow(query string, args ...interface{}) <-chan Values {
	future := make(chan Values, 1)
	go func() {
		defer close(future)
		future <- Pack(conn.QueryRowContext(conn.ctx, query, args...))
	}()
	return future
}

func (conn *Conn) AsyncExec(query string, args ...interface{}) <-chan Values {
	future := make(chan Values, 1)
	go func() {
		defer close(future)
		future <- Pack(conn.ExecContext(conn.ctx, query, args...))
	}()
	return future
}
