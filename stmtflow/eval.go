package stmtflow

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/zyguan/sqlz/resultset"
)

var (
	ErrConnExist    = errors.New("connection exist")
	ErrConnNotExist = errors.New("connection not exist")
	ErrConnBorrowed = errors.New("connection borrowed")
	ErrPollTimeout  = errors.New("poll timeout")
)

const (
	flagExist byte = 1 << iota
	flagInUse
)

type Pool struct {
	lock  sync.Mutex
	wg    sync.WaitGroup
	conns map[string]*sql.Conn
	flags map[string]byte
}

type BorrowedConn struct {
	*sql.Conn
	sess string
	pool *Pool
}

func (c *BorrowedConn) Return() error {
	p, s := c.pool, c.sess
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.flags[s]&flagExist == 0 {
		return ErrConnNotExist
	}
	p.flags[s] &^= flagInUse
	p.wg.Done()
	return nil
}

func (p *Pool) Put(s string, c *sql.Conn) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.flags[s]&flagExist > 0 {
		return ErrConnExist
	}
	p.flags[s] |= flagExist
	p.conns[s] = c
	return nil
}

func (p *Pool) Borrow(s string) (*BorrowedConn, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.flags[s]&flagExist == 0 {
		return nil, ErrConnNotExist
	}
	if p.flags[s]&flagInUse > 0 {
		return nil, ErrConnBorrowed
	}
	p.flags[s] |= flagInUse
	p.wg.Add(1)
	return &BorrowedConn{p.conns[s], s, p}, nil
}

func (p *Pool) Wait() { p.wg.Wait() }

func (p *Pool) Close() error {
	var fstErr error
	for _, c := range p.conns {
		if err := c.Close(); fstErr == nil && err != nil {
			fstErr = err
		}
	}
	return fstErr
}

type StmtStatus string

const (
	Pending StmtStatus = "Pending"
	Running StmtStatus = "Running"
	Done    StmtStatus = "Done"
)

type SessionStmt interface {
	Session() string
	Statement() Stmt
	Status() StmtStatus
	Result() Return
	Poll(ctx context.Context, c *BorrowedConn, w time.Duration) (SessionStmt, error)
}

const (
	S_QUERY uint = 1 << iota
	S_WAIT
	S_UNORDERED
)

type Stmt struct {
	Sess  string `json:"s"`
	SQL   string `json:"q"`
	Flags uint   `json:"flags,omitempty"`
}

func (s Stmt) Session() string { return s.Sess }

func (s Stmt) Statement() Stmt { return s }

func (s Stmt) Status() StmtStatus { return Pending }

func (s Stmt) Result() Return { return Return{} }

func (s Stmt) Poll(ctx context.Context, c *BorrowedConn, w time.Duration) (SessionStmt, error) {
	f := make(chan Return, 1)
	go func() {
		defer func() {
			c.Return()
			close(f)
		}()
		if s.Flags&S_QUERY > 0 {
			t0 := time.Now()
			rows, err := c.QueryContext(ctx, s.SQL)
			if err != nil {
				f <- Return{s, nil, WrapError(err), [2]time.Time{t0, time.Now()}}
				return
			}
			defer rows.Close()
			res, err := resultset.ReadFromRows(rows)
			f <- Return{s, res, WrapError(err), [2]time.Time{t0, time.Now()}}
		} else {
			t0 := time.Now()
			res, err := c.ExecContext(ctx, s.SQL)
			if err != nil {
				f <- Return{s, nil, WrapError(err), [2]time.Time{t0, time.Now()}}
				return
			}
			f <- Return{s, resultset.NewFromResult(res), nil, [2]time.Time{t0, time.Now()}}
		}
	}()
	r := RunningStmt{s, f}
	return r.Poll(ctx, c, w)
}

type RunningStmt struct {
	Stmt
	future <-chan Return
}

func (s RunningStmt) Status() StmtStatus { return Running }

func (s RunningStmt) Poll(ctx context.Context, c *BorrowedConn, w time.Duration) (SessionStmt, error) {
	if w <= 0 {
		select {
		case ret := <-s.future:
			return CompletedStmt{s.Stmt, ret}, nil
		case <-ctx.Done():
			return s, ctx.Err()
		}
	}
	select {
	case ret := <-s.future:
		return CompletedStmt{s.Stmt, ret}, nil
	case <-time.After(w):
		return s, ErrPollTimeout
	case <-ctx.Done():
		return s, ctx.Err()
	}
}

type CompletedStmt struct {
	Stmt
	ret Return
}

func (s CompletedStmt) Status() StmtStatus { return Done }

func (s CompletedStmt) Result() Return { return s.ret }

func (s CompletedStmt) Poll(ctx context.Context, c *BorrowedConn, w time.Duration) (SessionStmt, error) {
	return s, nil
}

type Block struct{}

type Resume struct{}

type Invoke struct {
	Stmt
}

type Return struct {
	Stmt
	Res *resultset.ResultSet
	Err error
	T   [2]time.Time
}

type Waitable interface{ Wait() }

type WaitableCloser interface {
	io.Closer
	Waitable
}

type EvalOptions struct {
	PingTime  time.Duration
	BlockTime time.Duration
	Callback  func(e Event)
}

func Run(ctx context.Context, db *sql.DB, stmts []Stmt, opts EvalOptions) error {
	w, err := Eval(ctx, db, stmts, opts)
	if w != nil {
		w.Wait()
		w.Close()
	}
	return err
}

func Eval(ctx context.Context, db *sql.DB, stmts []Stmt, opts EvalOptions) (WaitableCloser, error) {
	pool, head, err := initForEval(ctx, db, stmts)
	if err != nil {
		return nil, err
	}
	callback := opts.Callback
	if callback == nil {
		callback = func(_ Event) {}
	}
	for head.next != nil {
		for p := head; p.next != nil; p = p.next {
			stmt := p.next.stmt
			status := stmt.Status()

			if status == Pending {
				if stmt.Statement().Flags&S_WAIT > 0 && !p.waited {
					done := make(chan struct{})
					go func() {
						pool.Wait()
						close(done)
					}()
					select {
					case <-done:
						p.waited = true
					case <-ctx.Done():
						return pool, ctx.Err()
					}
					break
				}
				c, err := pool.Borrow(stmt.Session())
				if err != nil {
					if err == ErrConnBorrowed {
						continue
					}
					return pool, err
				}
				callback(NewInvokeEvent(stmt.Session(), Invoke{stmt.Statement()}))
				s, err := stmt.Poll(ctx, c, opts.BlockTime)
				if err != nil {
					if err == ErrPollTimeout {
						callback(NewBlockEvent(stmt.Session()))
						p.next.stmt = s
						continue
					}
					return pool, err
				}
				// Assert typeof(s) == CompletedStmt
				callback(NewReturnEvent(stmt.Session(), s.Result()))
				p.next = p.next.next
				break
			} else if status == Running {
				s, err := stmt.Poll(ctx, nil, opts.PingTime)
				if err != nil {
					if err == ErrPollTimeout {
						p.next.stmt = s
						continue
					}
					return pool, err
				}
				// Assert typeof(s) == CompletedStmt
				callback(NewResumeEvent(stmt.Session()))
				callback(NewReturnEvent(stmt.Session(), s.Result()))
				p.next = p.next.next
				break
			} else {
				return pool, errors.New("invalid statement status: " + string(stmt.Status()))
			}
		}
	}
	return pool, nil
}

type stmtNode struct {
	stmt SessionStmt
	next *stmtNode

	waited bool
}

func initForEval(ctx context.Context, db *sql.DB, stmts []Stmt) (*Pool, *stmtNode, error) {
	p := &Pool{
		conns: map[string]*sql.Conn{},
		flags: map[string]byte{},
	}
	h := &stmtNode{}
	m := make(map[string]bool, 2)
	for i := len(stmts) - 1; i >= 0; i-- {
		stmt := stmts[i]
		s := stmt.Session()
		h.next = &stmtNode{stmt, h.next, false}
		if !m[s] {
			c, err := db.Conn(ctx)
			if err != nil {
				return nil, nil, err
			}
			if err = p.Put(s, c); err != nil {
				return nil, nil, err
			}
			m[s] = true
		}
	}
	return p, h, nil
}
