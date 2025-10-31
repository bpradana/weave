package weave

import (
	"runtime"
	"sync"
)

// NewWorkerPoolDispatcher returns a Dispatcher that executes submitted tasks
// on a fixed-size worker pool. If size is zero or negative, GOMAXPROCS workers
// are used.
func NewWorkerPoolDispatcher(size int) Dispatcher {
	if size <= 0 {
		size = runtime.GOMAXPROCS(0)
		if size <= 0 {
			size = 1
		}
	}

	pool := &workerPoolDispatcher{
		tasks: make(chan func(), size*2),
	}
	pool.wg.Add(size)
	for i := 0; i < size; i++ {
		go pool.worker()
	}
	return pool
}

type workerPoolDispatcher struct {
	tasks chan func()
	wg    sync.WaitGroup
	once  sync.Once
}

func (d *workerPoolDispatcher) worker() {
	defer d.wg.Done()
	for fn := range d.tasks {
		if fn != nil {
			fn()
		}
	}
}

func (d *workerPoolDispatcher) Submit(fn func()) {
	d.tasks <- fn
}

func (d *workerPoolDispatcher) Stop() {
	d.once.Do(func() {
		close(d.tasks)
		d.wg.Wait()
	})
}
