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

type config struct {
	DSN, ModelURL                      string
	Concurrency, BatchSize             int
	Once, RetryFailed, EnqueueBackfill bool
	BackfillAfter                      string
	BackfillLimit                      int
}

func run(ctx context.Context, options config) error {
	dsn, modelURL, concurrency := options.DSN, options.ModelURL, options.Concurrency
	once, retryFailed := options.Once, options.RetryFailed
	if options.BatchSize < 1 || options.BatchSize > 8 {
		return errors.New("batch size must be between 1 and 8")
	}
	if options.EnqueueBackfill && retryFailed {
		return errors.New("choose one maintenance action")
	}

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
	if options.EnqueueBackfill {
		result, err := repo.EnqueueBackfill(ctx, options.BackfillAfter, options.BackfillLimit)
		if err == nil {
			slog.Info("backfill_enqueued", "result", string(result))
		}
		return err
	}
	if retryFailed {
		count, err := recommendations.RetryFailed(ctx)
		if err == nil {
			slog.Info("failed_jobs_rescheduled", "jobs", count)
		}
		return err
	}
	encoder := model.New(modelURL, embeddingModel)
	indexer := application.BatchIndexer{Repository: repo, Encoder: encoder, Kind: "live", Size: options.BatchSize}
	backfill := application.BatchIndexer{Repository: repo, Encoder: encoder, Kind: "backfill", Size: options.BatchSize}
	interactive := application.Personalizer{Repository: recommendations.Lane(0)}
	scheduled := application.Personalizer{Repository: recommendations.Lane(1)}
	var workers sync.WaitGroup
	var failed atomic.Bool
	for i := 0; i < concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			pipeline := application.Pipeline{Processors: []application.Processor{interactive, indexer, interactive, scheduled, backfill}}
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
	diagnostics := time.NewTicker(30 * time.Second)
	defer diagnostics.Stop()
	for {
		select {
		case <-ctx.Done():
			workers.Wait()
			return nil
		case <-diagnostics.C:
			snapshot, err := recommendations.Diagnostics(ctx)
			if err == nil {
				slog.Info("preparation_queues", "snapshot", string(snapshot))
			} else if ctx.Err() == nil {
				slog.Warn("preparation_diagnostics_failed")
			}
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
	concurrency := flag.Int("concurrency", 2, "parallel preparation tasks (1..8)")
	batchSize := flag.Int("batch-size", 4, "texts per model batch (1..8)")
	enqueueBackfill := flag.Bool("enqueue-backfill", false, "enqueue one bounded backfill range and exit")
	backfillAfter := flag.String("backfill-after", "", "exclusive post UUID cursor for backfill")
	backfillLimit := flag.Int("backfill-limit", 500, "posts to inspect for backfill (1..500)")
	once := flag.Bool("once", false, "process currently available jobs and exit")
	retryFailed := flag.Bool("retry-failed", false, "reschedule a bounded batch of exhausted jobs and exit")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	modelURL := os.Getenv("MODEL_URL")
	if modelURL == "" {
		modelURL = "http://127.0.0.1:8091"
	}
	if err := run(ctx, config{DSN: os.Getenv("DATABASE_URL"), ModelURL: modelURL, Concurrency: *concurrency, BatchSize: *batchSize, Once: *once, RetryFailed: *retryFailed, EnqueueBackfill: *enqueueBackfill, BackfillAfter: *backfillAfter, BackfillLimit: *backfillLimit}); err != nil {
		slog.Error("worker_stopped", "error", err)
		os.Exit(1)
	}
}
