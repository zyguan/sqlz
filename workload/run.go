package workload

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/sync/errgroup"
)

type Workload interface {
	Setup(ctx context.Context) error
	Teardown(err error) error
	Gen(rng *rand.Rand) interface{}
	Handle(evt interface{}) error
}

type RunOptions struct {
	Time    int `json:"time"`
	Rate    int `json:"rate"`
	QSize   int `json:"qsize"`
	Threads int `json:"threads"`

	Workload       Workload `json:"-"`
	AfterSetup     func()   `json:"-"`
	BeforeTeardown func()   `json:"-"`
}

func Run(ctx context.Context, opts RunOptions) (err error) {
	if opts.QSize < 0 {
		opts.QSize = 0
	}
	if opts.Threads < 1 {
		opts.Threads = 1
	}

	if err = opts.Workload.Setup(ctx); err != nil {
		return err
	}
	defer func() {
		if opts.BeforeTeardown != nil {
			opts.BeforeTeardown()
		}
		err = opts.Workload.Teardown(err)
	}()
	if opts.AfterSetup != nil {
		opts.AfterSetup()
	}

	if opts.Time > 0 {
		ctx, _ = context.WithTimeout(ctx, time.Duration(opts.Time)*time.Second)
	}

	events := make(chan interface{}, opts.QSize)
	g, failed := errgroup.WithContext(ctx)

	g.Go(func() (err error) {
		defer func() {
			close(events)
			if x := recover(); x != nil {
				if e, ok := x.(error); ok {
					err = e
				} else {
					err = fmt.Errorf("unexpected panic during generating workload event: %+v", x)
				}
			}
		}()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		if opts.Rate > 0 {
			ticker := time.NewTicker(time.Second / time.Duration(opts.Rate))
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-failed.Done():
					return
				case <-ticker.C:
					events <- opts.Workload.Gen(rng)
				}
			}
		} else {
			for {
				select {
				case <-ctx.Done():
					return
				case <-failed.Done():
					return
				case events <- opts.Workload.Gen(rng):
				}
			}
		}
	})

	for i := 0; i < opts.Threads; i++ {
		g.Go(func() (err error) {
			defer func() {
				if x := recover(); x != nil {
					if e, ok := x.(error); ok {
						err = e
					} else {
						err = fmt.Errorf("unexpected panic during handling workload event: %+v", x)
					}
				}
			}()
			for ev := range events {
				if err = opts.Workload.Handle(ev); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return g.Wait()
}
