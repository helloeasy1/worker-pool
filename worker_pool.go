// Package worker_pool provides a robust and efficient implementation of a worker pool
package worker_pool

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Test-only hook for pausing at a critical point.
// Has zero performance impact in production.
var submitInternalHook func()

// WorkerPool manages a pool of goroutines to execute tasks concurrently
type WorkerPool struct {
	numberOfWorkers int
	tasks           chan func()
	quit            chan struct{}
	workerWg        sync.WaitGroup
	taskWg          sync.WaitGroup
	stopOnce        sync.Once
	stopped         atomic.Bool
}

// NewWorkerPool creates and starts a new worker pool with a specified number of workers.
// The pool starts running immediately and is ready to accept tasks.
// It panics if numberOfWorkers is less than 1.
func NewWorkerPool(numberOfWorkers int) *WorkerPool {
	if numberOfWorkers < 1 {
		panic(fmt.Sprintf("worker_pool: number of workers must be greater than 0, got %d", numberOfWorkers))
	}

	wp := &WorkerPool{
		numberOfWorkers: numberOfWorkers,
		tasks:           make(chan func()),
		quit:            make(chan struct{}),
	}

	wp.workerWg.Add(numberOfWorkers)
	for range numberOfWorkers {
		go wp.worker()
	}

	return wp
}

// Worker is the function executed by each goroutine in the pool.
// It waits for tasks or a quit signal.
func (wp *WorkerPool) worker() {
	defer wp.workerWg.Done()
	for {
		select {
		case task, ok := <-wp.tasks:
			if !ok {
				// The tasks channel was closed by StopWait(), indicating no more tasks will be sent
				return
			}
			task()
		case <-wp.quit:
			// The quit channel was closed by Stop(), indicating an immediate shutdown
			return
		}
	}
}

// Returns true if the task was successfully submitted, and false otherwise (if the pool is stopped)
func (wp *WorkerPool) submitInternal(task func()) bool {
	if wp.stopped.Load() {
		return false
	}

	wp.taskWg.Add(1)
	wrapperTask := func() {
		defer wp.taskWg.Done()
		task()
	}

	if submitInternalHook != nil {
		submitInternalHook()
	}

	// A second check to avoid a race condition
	if wp.stopped.Load() {
		wp.taskWg.Done()
		return false
	}

	select {
	case wp.tasks <- wrapperTask:
		return true
	case <-wp.quit:
		// The pool was stopped (using Stop()) while we were waiting to send the task
		wp.taskWg.Done()
		return false
	}
}

// Submit adds a task to the worker pool for asynchronous execution.
// If the task is nil or the pool has been stopped, the task is silently ignored.
func (wp *WorkerPool) Submit(task func()) {
	if task == nil {
		return
	}
	wp.submitInternal(task)
}

// SubmitWait adds a task to the worker pool and blocks until its completion.
// If the task is nil or the pool has been stopped, the task is ignored and the function returns immediately.
func (wp *WorkerPool) SubmitWait(task func()) {
	if task == nil {
		return
	}
	if wp.stopped.Load() {
		return
	}

	var doneWg sync.WaitGroup
	doneWg.Add(1)

	wrapperTask := func() {
		defer doneWg.Done()
		task()
	}

	if wp.submitInternal(wrapperTask) {
		// Only wait if the task was successfully submitted
		doneWg.Wait()
	}
}

// Stop stops the worker pool, waiting only for the tasks that are currently
// executing to complete. Any tasks remaining in the queue are discarded.
// This method is idempotent and safe to call multiple times.
func (wp *WorkerPool) Stop() {
	wp.stopOnce.Do(func() {
		wp.stopped.Store(true)
		close(wp.quit)
		wp.workerWg.Wait()
	})
}

// StopWait stops the worker pool, waiting for all submitted tasks to complete,
// including those in the queue that have not yet started.
// This method is idempotent and safe to call multiple times.
func (wp *WorkerPool) StopWait() {
	wp.stopOnce.Do(func() {
		wp.stopped.Store(true)
		wp.taskWg.Wait()
		close(wp.tasks)
		wp.workerWg.Wait()
	})
}
