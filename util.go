package sqlz

import (
	"context"
	"database/sql"
	"io"
)

func Release(conn *sql.Conn) error {
	if conn == nil {
		return nil
	}
	err := conn.Raw(func(driverConn interface{}) error {
		if c, ok := driverConn.(io.Closer); ok {
			return c.Close()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err = conn.Close(); err != nil {
		return err
	}
	return nil
}

func WithConns(ctx context.Context, pool ConnPool, n int, f func(conns ...*sql.Conn) error) error {
	conns := make([]*sql.Conn, n)
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		for _, conn := range conns {
			Release(conn)
		}
	}()
	for i := 0; i < n; i++ {
		conn, err := pool.Conn(ctx)
		if err != nil {
			return err
		}
		conns[i] = conn
	}
	return f(conns...)
}

func WithStmtCache(p PreparerContext) *StmtPool {
	return &StmtPool{prepare: p, stmts: map[string]*sql.Stmt{}}
}

type StmtPool struct {
	prepare PreparerContext
	stmts   map[string]*sql.Stmt
}

func (p *StmtPool) Reset() {
	for q, s := range p.stmts {
		if s != nil {
			s.Close()
		}
		delete(p.stmts, q)
	}
}

func (p *StmtPool) GetStmt(ctx context.Context, query string) (StmtContext, error) {
	if s, ok := p.stmts[query]; ok {
		return s, nil
	}
	s, err := p.prepare.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	p.stmts[query] = s
	return s, nil
}

func (p *StmtPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := p.GetStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (p *StmtPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := p.GetStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func Fetch(q Queryer, query string, args ...interface{}) (*ResultSet, error) {
	rows, err := q.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return ReadFromRows(rows)
}

func FetchContext(ctx context.Context, q QueryerContext, query string, args ...interface{}) (*ResultSet, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return ReadFromRows(rows)
}

func MustFetch(q Queryer, query string, args ...interface{}) *ResultSet {
	rs, err := Fetch(q, query, args...)
	if err != nil {
		panic(err)
	}
	return rs
}

func MustFetchContext(ctx context.Context, q QueryerContext, query string, args ...interface{}) *ResultSet {
	rs, err := FetchContext(ctx, q, query, args...)
	if err != nil {
		panic(err)
	}
	return rs
}

func MustQuery(q Queryer, query string, args ...interface{}) *sql.Rows {
	rows, err := q.Query(query, args...)
	if err != nil {
		panic(err)
	}
	return rows
}

func MustQueryContext(ctx context.Context, q QueryerContext, query string, args ...interface{}) *sql.Rows {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		panic(err)
	}
	return rows
}

func MustExec(e Execer, query string, args ...interface{}) sql.Result {
	rows, err := e.Exec(query, args...)
	if err != nil {
		panic(err)
	}
	return rows
}

func MustExecContext(ctx context.Context, q ExecerContext, query string, args ...interface{}) sql.Result {
	rows, err := q.ExecContext(ctx, query, args...)
	if err != nil {
		panic(err)
	}
	return rows
}

func AsyncFetch(q Queryer, query string, args ...interface{}) Future {
	return AsyncCall(func() Values {
		return Pack(Fetch(q, query, args...))
	})
}

func AsyncFetchContext(ctx context.Context, q QueryerContext, query string, args ...interface{}) Future {
	return AsyncCall(func() Values {
		return Pack(FetchContext(ctx, q, query, args...))
	})
}

func AsyncExec(e Execer, query string, args ...interface{}) Future {
	return AsyncCall(func() Values {
		return Pack(e.Exec(query, args...))
	})
}

func AsyncExecContext(ctx context.Context, e ExecerContext, query string, args ...interface{}) Future {
	return AsyncCall(func() Values {
		return Pack(e.ExecContext(ctx, query, args...))
	})
}
