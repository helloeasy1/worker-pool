package worker_pool

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewWorkerPool verifies the constructor's behavior.
func TestNewWorkerPool(t *testing.T) {
	t.Run("panics on zero workers", func(t *testing.T) {
		assert.Panics(t, func() { NewWorkerPool(0) }, "NewWorkerPool(0) should panic")
	})

	t.Run("panics on negative workers", func(t *testing.T) {
		assert.Panics(t, func() { NewWorkerPool(-1) }, "NewWorkerPool(-1) should panic")
	})

	t.Run("creates pool with valid number of workers", func(t *testing.T) {
		wp := NewWorkerPool(4)
		defer wp.Stop()
		require.NotNil(t, wp)
		assert.Equal(t, 4, wp.numberOfWorkers)
		assert.NotNil(t, wp.tasks)
		assert.NotNil(t, wp.quit)
	})
}

// TestStopWait_GracefulShutdown verifies that StopWait executes all submitted tasks.
func TestStopWait_GracefulShutdown(t *testing.T) {
	var counter atomic.Int64
	numTasks := 1000
	numWorkers := 10

	wp := NewWorkerPool(numWorkers)

	for range numTasks {
		wp.Submit(func() {
			counter.Add(1)
			// Simulate a small amount of work
			time.Sleep(time.Microsecond)
		})
	}

	wp.StopWait()

	assert.Equal(t, int64(numTasks), counter.Load(), "StopWait should execute all submitted tasks")
}

// TestSubmitWait_BlocksUntilCompletion verifies that SubmitWait is synchronous.
func TestSubmitWait_BlocksUntilCompletion(t *testing.T) {
	wp := NewWorkerPool(2)
	defer wp.StopWait()

	var executed atomic.Bool
	taskDuration := 15 * time.Millisecond

	task := func() {
		time.Sleep(taskDuration)
		executed.Store(true)
	}

	start := time.Now()
	wp.SubmitWait(task)
	elapsed := time.Since(start)

	assert.True(t, executed.Load(), "SubmitWait should block until the task is completed")
	assert.GreaterOrEqual(t, elapsed, taskDuration, "SubmitWait should take at least as long as the task itself")
}

// TestStop_AbruptShutdown verifies that Stop completes in-flight tasks but discards queued ones.
// This test is carefully orchestrated to be deterministic and avoid deadlocks.
func TestStop_AbruptShutdown(t *testing.T) {
	var tasksCompleted atomic.Int64
	var tasksStarted atomic.Int64
	numWorkers := 4
	numTasksToSubmit := 20

	wp := NewWorkerPool(numWorkers)

	// A channel to pause workers, allowing us to control execution flow.
	taskHold := make(chan struct{})

	// Submit tasks in a separate goroutine to prevent the main test goroutine
	// from blocking when the pool becomes saturated.
	go func() {
		for range numTasksToSubmit {
			wp.Submit(func() {
				tasksStarted.Add(1)
				<-taskHold // Block worker until we close the channel.
				tasksCompleted.Add(1)
			})
		}
	}()

	// Wait until all workers have picked up a task and are blocked.
	require.Eventually(t, func() bool {
		return tasksStarted.Load() >= int64(numWorkers)
	}, 2*time.Second, 10*time.Millisecond, "workers did not start tasks in time")

	// Now, orchestrate the shutdown.
	// We must call Stop() and unblock the workers concurrently to avoid a deadlock,
	// as Stop() waits for the workers to finish, and the workers are waiting to be unblocked.
	var stopWg sync.WaitGroup
	stopWg.Add(1)
	go func() {
		defer stopWg.Done()
		wp.Stop()
	}()

	// Give a brief moment for the Stop() call to initiate. This is not strictly
	// necessary but makes the test's logic flow clearer.
	time.Sleep(20 * time.Millisecond)

	// Unblock the in-flight tasks. This allows the workers to complete their tasks and exit.
	close(taskHold)

	// Wait for the Stop() call to complete. This ensures the pool is fully shut down
	// before we proceed to the assertions.
	stopWg.Wait()

	// After Stop() returns, the counter should reflect only the tasks that were already executing.
	assert.Equal(t, int64(numWorkers), tasksCompleted.Load(), "Stop should only complete in-flight tasks")
}

// TestSubmitAfterShutdown verifies that tasks are ignored after the pool is stopped.
func TestSubmitAfterShutdown(t *testing.T) {
	testCases := []struct {
		name     string
		stopFunc func(wp *WorkerPool)
	}{
		{"After Stop", func(wp *WorkerPool) { wp.Stop() }},
		{"After StopWait", func(wp *WorkerPool) { wp.StopWait() }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var counter atomic.Int64
			wp := NewWorkerPool(2)

			wp.Submit(func() { counter.Add(1) })
			// Ensure the first task completes before shutdown.
			wp.taskWg.Wait()

			tc.stopFunc(wp)

			// These submissions should be silently ignored.
			wp.Submit(func() { counter.Add(10) })
			wp.SubmitWait(func() { counter.Add(100) })

			// Give a moment for any potential race conditions to manifest.
			time.Sleep(10 * time.Millisecond)

			assert.Equal(t, int64(1), counter.Load(), "Counter should not change after pool is stopped")
		})
	}
}

// TestShutdownIdempotency ensures that calling stop methods multiple times is safe.
func TestShutdownIdempotency(t *testing.T) {
	t.Run("Stop is idempotent", func(t *testing.T) {
		wp := NewWorkerPool(5)
		wp.Submit(func() { time.Sleep(5 * time.Millisecond) })

		var wg sync.WaitGroup
		// Call Stop concurrently from multiple goroutines.
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.NotPanics(t, func() { wp.Stop() })
			}()
		}
		wg.Wait()
	})

	t.Run("StopWait is idempotent", func(t *testing.T) {
		wp := NewWorkerPool(5)
		wp.Submit(func() { time.Sleep(5 * time.Millisecond) })

		var wg sync.WaitGroup
		// Call StopWait concurrently from multiple goroutines.
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.NotPanics(t, func() { wp.StopWait() })
			}()
		}
		wg.Wait()
	})
}

// TestRaceCondition simulates a high-contention environment to be run with `go test -race`.
func TestRaceCondition(t *testing.T) {
	numWorkers := 8
	numSubmitters := 50
	tasksPerSubmitter := 100
	var tasksExecuted atomic.Int64

	wp := NewWorkerPool(numWorkers)

	var submittersWg sync.WaitGroup
	submittersWg.Add(numSubmitters)

	// Spawn many goroutines to submit tasks concurrently.
	for range numSubmitters {
		go func() {
			defer submittersWg.Done()
			for j := range tasksPerSubmitter {
				// Mix in different submission types to exercise all code paths.
				if j%10 == 0 {
					wp.SubmitWait(func() {
						tasksExecuted.Add(1)
					})
				} else {
					wp.Submit(func() {
						tasksExecuted.Add(1)
					})
				}
			}
		}()
	}

	// Wait for all tasks to be submitted before shutting down.
	submittersWg.Wait()
	wp.StopWait()

	expectedTotal := int64(numSubmitters * tasksPerSubmitter)
	assert.Equal(t, expectedTotal, tasksExecuted.Load(), "All submitted tasks should be executed under high contention")
}

// TestSubmittingNilTaskIsANoOp verifies that submitting a nil task is safely ignored
// and does not impact the health or operation of the worker pool.
func TestSubmittingNilTaskIsANoOp(t *testing.T) {
	var counter atomic.Int64
	wp := NewWorkerPool(2)
	defer wp.StopWait()

	// 1. Submit a valid task to ensure the pool is working.
	wp.SubmitWait(func() { counter.Add(1) })
	require.Equal(t, int64(1), counter.Load())

	// 2. Submit nil tasks via both methods. This should do nothing.
	assert.NotPanics(t, func() {
		wp.Submit(nil)
		wp.SubmitWait(nil)
	})

	// 3. Submit another valid task to prove the pool is still healthy and running.
	wp.SubmitWait(func() { counter.Add(1) })
	require.Equal(t, int64(2), counter.Load())

	// The StopWait will hang if the pool is in a bad state, providing an implicit check.
}

// --- Benchmarks ---

// BenchmarkWorkerPool measures the throughput of the worker pool under various conditions.
func BenchmarkWorkerPool(b *testing.B) {
	// A trivial task for measuring submission overhead.
	trivialTask := func() {}

	// A task that simulates some CPU work.
	cpuWorkTask := func() {
		// This loop is just to consume CPU cycles.
		sum := 0
		for i := range 100 {
			sum += i
		}
	}

	scenarios := []struct {
		name       string
		numWorkers int
		task       func()
	}{
		{"1Worker_TrivialTask", 1, trivialTask},
		{"4Workers_TrivialTask", 4, trivialTask},
		{"16Workers_TrivialTask", 16, trivialTask},
		{"1Worker_CpuWork", 1, cpuWorkTask},
		{"4Workers_CpuWork", 4, cpuWorkTask},
		{"16Workers_CpuWork", 16, cpuWorkTask},
	}

	for _, s := range scenarios {
		b.Run(fmt.Sprintf("%s", s.name), func(b *testing.B) {
			wp := NewWorkerPool(s.numWorkers)
			defer wp.StopWait()

			b.ResetTimer()

			// b.N is the number of iterations the benchmark will run.
			// RunParallel submits tasks from multiple goroutines concurrently.
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					wp.Submit(s.task)
				}
			})
		})
	}
}
