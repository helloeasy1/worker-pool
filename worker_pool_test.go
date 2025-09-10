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

func TestStopWait_GracefulShutdown(t *testing.T) {
	var counter atomic.Int64
	numTasks := 1000
	numWorkers := 10

	wp := NewWorkerPool(numWorkers)

	for range numTasks {
		wp.Submit(func() {
			counter.Add(1)
			time.Sleep(time.Microsecond)
		})
	}

	wp.StopWait()

	assert.Equal(t, int64(numTasks), counter.Load(), "StopWait should execute all submitted tasks")
}

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

func TestStop_AbruptShutdown(t *testing.T) {
	var tasksCompleted atomic.Int64
	var tasksStarted atomic.Int64
	numWorkers := 4
	numTasksToSubmit := 20

	wp := NewWorkerPool(numWorkers)

	// A channel to pause workers, allowing us to control execution flow
	taskHold := make(chan struct{})

	// Submit tasks in a separate goroutine to prevent the main test goroutine from blocking
	go func() {
		for range numTasksToSubmit {
			wp.Submit(func() {
				tasksStarted.Add(1)
				<-taskHold // Block worker until we close the channel.
				tasksCompleted.Add(1)
			})
		}
	}()

	require.Eventually(t, func() bool {
		return tasksStarted.Load() >= int64(numWorkers)
	}, 2*time.Second, 10*time.Millisecond, "workers did not start tasks in time")

	// Call Stop() and unblock the workers concurrently to avoid a deadlock
	var stopWg sync.WaitGroup
	stopWg.Add(1)
	go func() {
		defer stopWg.Done()
		wp.Stop()
	}()

	time.Sleep(20 * time.Millisecond)

	// Unblock the in-flight tasks
	close(taskHold)

	stopWg.Wait()

	assert.Equal(t, int64(numWorkers), tasksCompleted.Load(), "Stop should only complete in-flight tasks")
}

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
			wp.taskWg.Wait()

			tc.stopFunc(wp)

			wp.Submit(func() { counter.Add(10) })
			wp.SubmitWait(func() { counter.Add(100) })

			// A moment for any potential race conditions
			time.Sleep(10 * time.Millisecond)

			assert.Equal(t, int64(1), counter.Load(), "Counter should not change after pool is stopped")
		})
	}
}

func TestShutdownIdempotency(t *testing.T) {
	t.Run("Stop is idempotent", func(t *testing.T) {
		wp := NewWorkerPool(5)
		wp.Submit(func() { time.Sleep(5 * time.Millisecond) })

		var wg sync.WaitGroup
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

	for range numSubmitters {
		go func() {
			defer submittersWg.Done()
			for j := range tasksPerSubmitter {
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

	submittersWg.Wait()
	wp.StopWait()

	expectedTotal := int64(numSubmitters * tasksPerSubmitter)
	assert.Equal(t, expectedTotal, tasksExecuted.Load(), "All submitted tasks should be executed under high contention")
}

func TestSubmittingNilTaskIsANoOp(t *testing.T) {
	var counter atomic.Int64
	wp := NewWorkerPool(2)
	defer wp.StopWait()

	wp.SubmitWait(func() { counter.Add(1) })
	require.Equal(t, int64(1), counter.Load())

	assert.NotPanics(t, func() {
		wp.Submit(nil)
		wp.SubmitWait(nil)
	})

	wp.SubmitWait(func() { counter.Add(1) })
	require.Equal(t, int64(2), counter.Load())
}

// --- Benchmarks ---

func BenchmarkWorkerPool(b *testing.B) {
	trivialTask := func() {}

	cpuWorkTask := func() {
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

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					wp.Submit(s.task)
				}
			})
		})
	}
}
