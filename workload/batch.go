package workload

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"math/rand"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	ErrTaskCanceled = errors.New("task canceled")
)

type BatchOptions struct {
	Records    int `json:"records"`
	Threads    int `json:"threads"`
	BatchSize  int `json:"batch_size"`
	RetryLimit int `json:"retry_limit"`

	OnBatch func(b *Batch) error               `json:"-"`
	OnTick  func(task int, cur int, total int) `json:"-"`
}

type Batch struct {
	context.Context
	Task    int
	Records int
	Range   [2]int
	Conn    *sql.Conn
	Rand    *rand.Rand
	Buf     *bytes.Buffer
}

func (b *Batch) Exec(query string, args ...interface{}) (sql.Result, error) {
	return b.Conn.ExecContext(b, query, args...)
}

func (b *Batch) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return b.Conn.QueryContext(b, query, args...)
}

func (b *Batch) QueryRow(query string, args ...interface{}) *sql.Row {
	return b.Conn.QueryRowContext(b, query, args...)
}

func BatchLoad(ctx context.Context, db *sql.DB, opts BatchOptions) error {
	if opts.OnBatch == nil {
		return errors.New("on batch callback is required")
	}
	if opts.Records < 1 {
		return nil
	}
	if opts.Threads < 1 {
		opts.Threads = 2 * runtime.NumCPU()
	}
	if opts.BatchSize < 1 {
		opts.BatchSize = 50
	}
	if opts.RetryLimit < 0 {
		opts.RetryLimit = 0
	}

	ticks := time.NewTicker(5 * time.Second)
	defer ticks.Stop()

	var g errgroup.Group
	for i := 0; i < opts.Threads; i++ {
		task := batchTask{
			id:    i,
			db:    db,
			ctx:   ctx,
			opts:  opts,
			ticks: ticks,
		}
		g.Go(task.run)
	}

	return g.Wait()
}

type batchTask struct {
	id    int
	db    *sql.DB
	ctx   context.Context
	opts  BatchOptions
	ticks *time.Ticker
}

func (t *batchTask) run() (err error) {
	b := Batch{
		Context: t.ctx,
		Records: t.opts.Records,
		Task:    t.id,
		Rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
		Buf:     new(bytes.Buffer),
	}
	defer func() {
		if b.Conn != nil {
			b.Conn.Close()
		}
	}()

	total := t.opts.Records / t.opts.Threads
	offset := total * t.id
	if remainder := t.opts.Records % t.opts.Threads; t.id < remainder {
		total += 1
		offset += t.id
	} else {
		offset += remainder
	}
	end := offset + total

	for k := offset; k < end; k = b.Range[1] {
		b.Range[0], b.Range[1] = k, k+t.opts.BatchSize
		if b.Range[1] > end {
			b.Range[1] = end
		}
		if err := t.doBatch(&b, 0); err != nil {
			return err
		}
		select {
		case <-t.ticks.C:
			if t.opts.OnTick != nil {
				t.opts.OnTick(t.id, k-offset, total)
			}
		case <-t.ctx.Done():
			return ErrTaskCanceled
		default:
		}
	}
	return nil
}

func (t *batchTask) doBatch(b *Batch, retry int) error {
	backoff := func() error {
		d := math.Min(50*math.Pow(2, float64(retry)), 10000)
		select {
		case <-time.After(time.Duration(d) * time.Millisecond):
			return t.doBatch(b, retry+1)
		case <-t.ctx.Done():
			return ErrTaskCanceled
		}
	}
	if b.Conn == nil && retry < t.opts.RetryLimit+1 {
		for {
			conn, err := t.db.Conn(t.ctx)
			if err == nil {
				err = conn.PingContext(t.ctx)
				if err == nil {
					b.Conn = conn
					break
				}
				conn.Close()
			} else if isRetryable(err) {
				return backoff()
			} else {
				return err
			}
		}
	}
	if b.Conn == nil {
		return errors.New("exceed retry limit")
	}

	b.Buf.Reset()

	if err := t.opts.OnBatch(b); err == nil {
		return nil
	} else if isRetryable(err) {
		if b.Conn != nil {
			b.Conn.Close()
			b.Conn = nil
		}
		return backoff()
	} else {
		return err
	}
}

func isRetryable(err error) bool {
	return errors.Is(err, driver.ErrBadConn)
}
