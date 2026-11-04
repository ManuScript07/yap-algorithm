package application

import (
	"context"
	"fmt"
	"time"

	"yap-algorithm/internal/domain"
)

// IndexJob is a leased content snapshot. Text is empty when the job has already
// been superseded or the post/author is no longer eligible for processing.
type IndexJob struct {
	ID                  int64
	Lease, PostID, Text string
	Version             int
}

type IndexRepository interface {
	Claim(context.Context) (IndexJob, bool, error)
	// Complete rejects expired/replaced leases and checks the current post
	// version in the same transaction as publishing the embedding.
	Complete(context.Context, IndexJob, []float64) (bool, error)
	Retry(context.Context, IndexJob) error
	Cleanup(context.Context) error
}

type TextEncoder interface {
	Encode(context.Context, string) ([]float64, error)
}

type Indexer struct {
	Repository IndexRepository
	Encoder    TextEncoder
}

func (w Indexer) Once(ctx context.Context) (bool, error) {
	job, found, err := w.Repository.Claim(ctx)
	if err != nil || !found {
		return false, err
	}
	var vector []float64
	if job.Text != "" {
		vector, err = w.Encoder.Encode(ctx, job.Text)
		if err == nil {
			vector, err = domain.NormalizeVector(vector)
		}
	}
	if err == nil {
		_, err = w.Repository.Complete(ctx, job, vector)
	}
	if err != nil && ctx.Err() == nil {
		// Release a failed job with durable backoff even if the query that failed
		// exhausted its own deadline. Shutdown leaves the lease to expire.
		retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		retryErr := w.Repository.Retry(retryCtx, job)
		cancel()
		if retryErr != nil {
			return true, fmt.Errorf("index: %w; release: %w", err, retryErr)
		}
	}
	return true, err
}
