// worker is an internal Supabase processor. It has no client-facing listener and
// does not apply migrations. Content and permissions remain owned by Supabase.
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

	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/model"
)

const embeddingModel = "intfloat/multilingual-e5-small@614241f622f53c4eeff9890bdc4f31cfecc418b3:onnx-int8:query"

func run(ctx context.Context, dsn, modelURL string, concurrency int, once, retryFailed bool) error {
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	if concurrency < 1 || concurrency > 8 {
		return errors.New("concurrency must be between 1 and 8")
	}
	repo, err := postgres.OpenIndexRepository(ctx, dsn, embeddingModel, int32(concurrency+2))
	if err != nil {
		return err
	}
	defer repo.Close()
	recommendations, err := repo.Recommendations(ctx)
	if err != nil {
		return err
	}
	if retryFailed {
		count, err := recommendations.RetryFailed(ctx)
		if err == nil {
			slog.Info("failed_jobs_rescheduled", "jobs", count)
		}
		return err
	}
	indexer := application.Indexer{Repository: repo, Encoder: model.New(modelURL, embeddingModel)}
	personalizer := application.Personalizer{Repository: recommendations}
	var workers sync.WaitGroup
	var failed atomic.Bool
	for i := 0; i < concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			pipeline := application.Pipeline{Processors: []application.Processor{indexer, personalizer}}
			for ctx.Err() == nil {
				jobCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
				worked, err := pipeline.Once(jobCtx)
				cancel()
				if err != nil && ctx.Err() == nil {
					failed.Store(true)
					// Error values may contain connection details or user content.
					// Keep runtime logs free of raw database/model payloads.
					slog.Warn("pipeline_job_failed")
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
			return errors.New("one or more index jobs failed; see the private job queue")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	// Cleanup has its own schedule, so a continuous backlog cannot starve it.
	cleanup := time.NewTicker(time.Minute)
	defer cleanup.Stop()
	for {
		select {
		case <-ctx.Done():
			workers.Wait()
			return nil
		case <-cleanup.C:
			if err := repo.Cleanup(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("index_cleanup_failed")
			}
			if err := recommendations.CleanupRecommendations(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("recommendation_cleanup_failed")
			}
		}
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	concurrency := flag.Int("concurrency", 2, "parallel index jobs (1..8)")
	once := flag.Bool("once", false, "process currently available jobs and exit")
	retryFailed := flag.Bool("retry-failed", false, "reschedule a bounded batch of exhausted jobs and exit")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	modelURL := os.Getenv("MODEL_URL")
	if modelURL == "" {
		modelURL = "http://127.0.0.1:8091"
	}
	if err := run(ctx, os.Getenv("DATABASE_URL"), modelURL, *concurrency, *once, *retryFailed); err != nil {
		slog.Error("worker_stopped", "error", err)
		os.Exit(1)
	}
}
