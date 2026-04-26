package app

import (
	"runtime"
	"time"

	"go.uber.org/zap"
)

type runtimeStatsRecorder struct {
	startedAt time.Time
	start     runtime.MemStats
}

func newRuntimeStatsRecorder(now time.Time) runtimeStatsRecorder {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return runtimeStatsRecorder{
		startedAt: now,
		start:     mem,
	}
}

func (r runtimeStatsRecorder) Log(logger *zap.Logger, pipelineID string, runID string) {
	var end runtime.MemStats
	runtime.ReadMemStats(&end)

	elapsed := time.Since(r.startedAt)
	allocDelta := end.TotalAlloc - r.start.TotalAlloc
	pauseDelta := end.PauseTotalNs - r.start.PauseTotalNs
	numGCDelta := end.NumGC - r.start.NumGC

	var allocRate float64
	if elapsed > 0 {
		allocRate = float64(allocDelta) / elapsed.Seconds()
	}

	logger.Info("runtime stats",
		zap.String("pipeline_id", pipelineID),
		zap.String("run_id", runID),
		zap.Duration("uptime", elapsed),
		zap.Uint64("heap_alloc_bytes", end.HeapAlloc),
		zap.Uint64("heap_inuse_bytes", end.HeapInuse),
		zap.Uint64("heap_idle_bytes", end.HeapIdle),
		zap.Uint64("heap_released_bytes", end.HeapReleased),
		zap.Uint64("heap_sys_bytes", end.HeapSys),
		zap.Uint64("stack_inuse_bytes", end.StackInuse),
		zap.Uint64("total_alloc_bytes", end.TotalAlloc),
		zap.Uint64("alloc_delta_bytes", allocDelta),
		zap.Float64("alloc_rate_bytes_per_sec", allocRate),
		zap.Uint32("gc_cycles", numGCDelta),
		zap.Uint64("gc_pause_total_ns", pauseDelta),
		zap.Uint64("mallocs", end.Mallocs),
		zap.Uint64("frees", end.Frees),
		zap.Int("goroutines", runtime.NumGoroutine()),
	)
}
