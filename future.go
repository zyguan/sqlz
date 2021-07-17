package sqlz

import "time"

type Values []interface{}

func (xs Values) Error() error {
	if len(xs) > 0 {
		if e, ok := xs[len(xs)-1].(error); ok {
			return e
		}
	}
	return nil
}

func (xs Values) Nth(i int) interface{} {
	if i < 0 {
		i += len(xs)
	}
	if i < 0 || i >= len(xs) {
		return nil
	}
	return xs[i]
}

func Pack(xs ...interface{}) Values { return xs }

type Future struct {
	result Values
	panic  interface{}
	done   chan struct{}
}

func (f *Future) Get(timeout time.Duration) (Values, interface{}, bool) {
	if timeout >= 0 {
		select {
		case <-time.After(timeout):
			return nil, nil, false
		case <-f.done:
			return f.result, f.panic, true
		}
	} else {
		<-f.done
		return f.result, f.panic, true
	}
}

func AsyncCall(f func() Values) Future {
	future := Future{done: make(chan struct{})}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				future.panic = r
			}
			close(future.done)
		}()
		future.result = f()
	}()
	return future
}
