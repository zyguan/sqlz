package sqlz

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/zyguan/sqlz/resultset"
)

type ExecStatus string

const (
	ExecDone    = "Done"
	ExecFailed  = "Failed"
	ExecRunning = "Running"
	ExecBlocked = "Blocked"
)

type ExecOptions struct {
	WaitBefore time.Duration
	WaitAfter  time.Duration
	Callback   func(r *resultset.ResultSet, e error)
	IsQuery    func(s string) bool
}

type Executor struct {
	ctx context.Context
	db  *sql.DB
	m   map[string]*sql.Conn
	x   map[string]chan struct{}
}

func NewExecutor(ctx context.Context, db *sql.DB) *Executor {
	return &Executor{
		ctx: ctx,
		db:  db,
		m:   map[string]*sql.Conn{},
		x:   map[string]chan struct{}{},
	}
}

func (ex *Executor) Execute(s string, next func() string, opts ExecOptions) (ExecStatus, error) {
	c, ok := ex.m[s]
	if !ok {
		if x, err := Connect(ex.ctx, ex.db); err == nil {
			ex.m[s], c = x, x
		} else {
			return ExecFailed, err
		}
	}
	if f, ok := ex.x[s]; ok {
		if opts.WaitBefore > 0 {
			select {
			case <-f:
				delete(ex.x, s)
			case <-time.After(opts.WaitBefore):
				return ExecBlocked, nil
			}
		} else {
			<-f
			delete(ex.x, s)
		}
	}

	if opts.Callback == nil {
		opts.Callback = func(r *resultset.ResultSet, e error) {}
	}
	if opts.IsQuery == nil {
		opts.IsQuery = isQuery
	}

	ex.x[s] = make(chan struct{})
	go func() {
		defer close(ex.x[s])
		q := next()
		if opts.IsQuery(q) {
			rows, err := c.QueryContext(ex.ctx, q)
			if err != nil {
				opts.Callback(nil, err)
				return
			}
			defer rows.Close()
			opts.Callback(resultset.ReadFromRows(rows))
		} else {
			res, err := c.ExecContext(ex.ctx, q)
			if err != nil {
				opts.Callback(nil, err)
				return
			}
			opts.Callback(resultset.NewFromResult(res), nil)
		}
	}()

	if opts.WaitAfter <= 0 {
		<-ex.x[s]
		delete(ex.x, s)
		return ExecDone, nil
	}

	select {
	case <-ex.x[s]:
		delete(ex.x, s)
		return ExecDone, nil
	case <-time.After(opts.WaitAfter):
		return ExecRunning, nil
	}
}

func (ex *Executor) Shutdown() {
	for n, c := range ex.m {
		if ch, ok := ex.x[n]; ok {
			<-ch
			delete(ex.x, n)
		}
		c.Close()
	}
}

func isQuery(sql string) bool {
	sql = strings.ToLower(strings.TrimLeft(strings.TrimSpace(sql), "("))
	for _, w := range []string{"select ", "show ", "admin show ", "explain ", "desc ", "describe "} {
		if strings.HasPrefix(sql, w) {
			return true
		}
	}
	return false
}
