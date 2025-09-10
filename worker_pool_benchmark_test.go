package worker_pool

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// --- Task Definitions ---

var trivialTask = func() {}

var cpuBoundTask = func() {
	var counter int
	for range 1000 {
		counter++
	}
}

var ioBoundTask = func() {
	time.Sleep(1 * time.Millisecond)
}

// --- Worker Pool Benchmarks ---

func BenchmarkSubmit(b *testing.B) {
	tasks := map[string]func(){
		"Trivial": cpuBoundTask,
		"CPU":     cpuBoundTask,
		"IO":      ioBoundTask,
	}

	workerCounts := []int{1, 4, runtime.NumCPU(), 32, 128}
	submitterCounts := []int{1, runtime.NumCPU(), 32}

	for taskName, taskFunc := range tasks {
		for _, numWorkers := range workerCounts {
			for _, numSubmitters := range submitterCounts {
				benchName := fmt.Sprintf("Task=%s/Workers=%03d/Submitters=%03d", taskName, numWorkers, numSubmitters)

				b.Run(benchName, func(b *testing.B) {
					pool := NewWorkerPool(numWorkers)
					defer pool.StopWait()

					b.SetParallelism(numSubmitters)
					b.ResetTimer()

					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							pool.Submit(taskFunc)
						}
					})
				})
			}
		}
	}
}

func BenchmarkSubmitWait(b *testing.B) {
	// For SubmitWait, the task duration dominates the result, so we focus on a CPU-bound task.
	taskFunc := cpuBoundTask
	workerCounts := []int{1, 4, runtime.NumCPU(), 32}

	for _, numWorkers := range workerCounts {
		benchName := fmt.Sprintf("Task=CPU/Workers=%03d", numWorkers)
		b.Run(benchName, func(b *testing.B) {
			pool := NewWorkerPool(numWorkers)
			defer pool.StopWait()

			b.ResetTimer()

			for b.Loop() {
				pool.SubmitWait(taskFunc)
			}
		})
	}
}

// --- Baseline Comparison ---

func BenchmarkGoroutinePerTask(b *testing.B) {
	tasks := map[string]func(){
		"Trivial": trivialTask,
		"CPU":     cpuBoundTask,
		"IO":      ioBoundTask,
	}

	for taskName, taskFunc := range tasks {
		b.Run(fmt.Sprintf("Task=%s", taskName), func(b *testing.B) {
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				var wg sync.WaitGroup
				for pb.Next() {
					wg.Go(func() {
						taskFunc()
					})
				}
				wg.Wait()
			})
		})
	}
}
