// media-worker is separate from semantic indexing: image decoding has its own
// small concurrency/memory budget and narrowly scoped Storage/database identity.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"yap-algorithm/internal/adapters/photo"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/adapters/storage"
	"yap-algorithm/internal/application"
)

func run(parent context.Context, concurrency int, once, retry, status bool) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if concurrency < 1 || concurrency > 2 {
		return errors.New("media concurrency must be 1..2")
	}
	repo, err := postgres.OpenMediaRepository(ctx, os.Getenv("MEDIA_DATABASE_URL"), int32(concurrency+1))
	if err != nil {
		return err
	}
	defer repo.Close()
	if retry {
		return repo.Maintenance(ctx, true)
	}
	if status {
		snapshot, err := repo.Snapshot(ctx)
		if err == nil {
			slog.Info("media_queue_status", "queue", snapshot)
		}
		return err
	}
	objects, err := storage.NewMedia(os.Getenv("SUPABASE_URL"), os.Getenv("SUPABASE_PUBLISHABLE_KEY"), os.Getenv("MEDIA_STORAGE_TOKEN"))
	if err != nil {
		return err
	}
	if err := repo.Register(ctx, concurrency); err != nil {
		return err
	}
	defer func() {
		release, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		_ = repo.Unregister(release)
	}()
	processor := application.PhotoProcessor{Repository: repo, Storage: objects, Preparer: photo.Processor{}}
	cleaner := application.MediaCleaner{Repository: repo, Storage: objects}
	var workers sync.WaitGroup
	var failed atomic.Bool
	var heartbeatLost atomic.Bool
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		timer := time.NewTicker(5 * time.Second)
		defer timer.Stop()
		var ticks int
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if err := repo.Heartbeat(ctx); err != nil {
					if ctx.Err() == nil {
						heartbeatLost.Store(true)
					}
					cancel()
					return
				}
				ticks++
				if ticks%6 == 0 {
					if snapshot, err := repo.Snapshot(ctx); err == nil {
						var memory runtime.MemStats
						runtime.ReadMemStats(&memory)
						slog.Info("media_queue_status", "queue", snapshot, "go_heap_bytes", memory.HeapAlloc,
							"go_sys_bytes", memory.Sys, "goroutines", runtime.NumGoroutine(), "db_pool", repo.PoolUsage())
					}
				}
			}
		}
	}()
	defer func() { cancel(); <-heartbeatDone }()
	for i := 0; i < concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			pipeline := application.Pipeline{Processors: []application.Processor{processor, cleaner}}
			for ctx.Err() == nil {
				jobCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
				worked, err := pipeline.Once(jobCtx)
				cancel()
				if err != nil && ctx.Err() == nil {
					failed.Store(true)
					slog.Warn("media_job_failed")
				}
				if once && (!worked || err != nil) {
					return
				}
				if !worked || err != nil {
					select {
					case <-ctx.Done():
						return
					case <-time.After(500 * time.Millisecond):
					}
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			if heartbeatLost.Load() {
				return errors.New("media worker registration lost")
			}
			if once && failed.Load() {
				return errors.New("one or more media jobs failed")
			}
			if parent.Err() == nil && ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		case <-ticker.C:
			if err := repo.Maintenance(ctx, false); err != nil && ctx.Err() == nil {
				slog.Warn("media_maintenance_failed")
			}
		}
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	concurrency := flag.Int("concurrency", 1, "parallel photo/cleanup tasks (1..2)")
	once := flag.Bool("once", false, "drain available jobs and exit")
	retry := flag.Bool("retry-failed", false, "reschedule a bounded batch of exhausted jobs and exit")
	status := flag.Bool("status", false, "print aggregate queue diagnostics and exit")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *concurrency, *once, *retry, *status); err != nil {
		slog.Error("media_worker_stopped", "error", err)
		os.Exit(1)
	}
}
