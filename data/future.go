package data

import (
	"context"
	"fmt"
	"sync"
)

type Future interface {
	Ready() bool
	Get(ctx context.Context) (interface{}, error)
}

type SyncFuture struct {
	val interface{}
	err error
}

func (s SyncFuture) Ready() bool {
	return true
}

func (s *SyncFuture) Get(_ context.Context) (interface{}, error) {
	return s.val, s.err
}

func NewSyncFuture(val interface{}, err error) *SyncFuture {
	return &SyncFuture{
		val: val,
		err: err,
	}
}

var AsyncFutureCanceledErr error = fmt.Errorf("async future was canceled")

type AsyncFuture struct {
	sync.Mutex
	doneChannel chan bool
	cancelFn    context.CancelFunc
	val         interface{}
	err         error
}

func (f *AsyncFuture) set(val interface{}, err error) {
	f.Lock()
	defer f.Unlock()
	f.val = val
	f.err = err
	f.doneChannel <- true
}

func (f *AsyncFuture) get() (interface{}, error) {
	f.Lock()
	defer f.Unlock()
	return f.val, f.err
}

func (f *AsyncFuture) Ready() bool {
	return true
}

func (f *AsyncFuture) Get(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		f.cancelFn()
		return nil, AsyncFutureCanceledErr
	case <-f.doneChannel:
		return f.get()
	}
}

func NewAsyncFuture(ctx context.Context, closure func(context.Context) (interface{}, error)) *AsyncFuture {
	childCtx, cancel := context.WithCancel(ctx)
	f := &AsyncFuture{
		doneChannel: make(chan bool),
		cancelFn:    cancel,
	}

	go func(ctx2 context.Context) {
		val, err := closure(ctx2)
		f.set(val, err)
	}(childCtx)
}
