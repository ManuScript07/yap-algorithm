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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"yap-algorithm/internal/adapters/photo"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/adapters/storage"
	"yap-algorithm/internal/application"
)

func run(ctx context.Context, concurrency int, once, retry bool) error {
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
	objects, err := storage.NewMedia(os.Getenv("SUPABASE_URL"), os.Getenv("SUPABASE_PUBLISHABLE_KEY"), os.Getenv("MEDIA_STORAGE_TOKEN"))
	if err != nil {
		return err
	}
	processor := application.PhotoProcessor{Repository: repo, Storage: objects, Preparer: photo.Processor{}}
	cleaner := application.MediaCleaner{Repository: repo, Storage: objects}
	var workers sync.WaitGroup
	var failed atomic.Bool
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
	if once {
		workers.Wait()
		if failed.Load() {
			return errors.New("one or more media jobs failed")
		}
		return ctx.Err()
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			workers.Wait()
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
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *concurrency, *once, *retry); err != nil {
		slog.Error("media_worker_stopped", "error", err)
		os.Exit(1)
	}
}
