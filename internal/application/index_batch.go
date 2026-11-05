package application

import (
	"context"
	"errors"
	"time"

	"yap-algorithm/internal/domain"
)

type IndexCompletion struct {
	Job    IndexJob
	Vector []float64
}

type BatchIndexRepository interface {
	ClaimBatch(context.Context, string, int) ([]IndexJob, error)
	CompleteBatch(context.Context, []IndexCompletion) error
	Retry(context.Context, IndexJob) error
}

type BatchTextEncoder interface {
	EncodeBatch(context.Context, []string) ([][]float64, error)
}

// BatchIndexer keeps model inference outside the database transaction. A batch
// has a fixed upper bound; every completion retains its own source/lease fence.
type BatchIndexer struct {
	Repository BatchIndexRepository
	Encoder    BatchTextEncoder
	Kind       string
	Size       int
}

func (w BatchIndexer) Once(ctx context.Context) (bool, error) {
	if w.Size < 1 || w.Size > 8 || (w.Kind != "live" && w.Kind != "backfill") {
		return false, errors.New("invalid index lane or batch size")
	}
	jobs, err := w.Repository.ClaimBatch(ctx, w.Kind, w.Size)
	if err != nil || len(jobs) == 0 {
		return false, err
	}
	texts := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if job.Text != "" {
			texts = append(texts, job.Text)
		}
	}
	var vectors [][]float64
	if len(texts) > 0 {
		vectors, err = w.Encoder.EncodeBatch(ctx, texts)
		if err == nil && len(vectors) != len(texts) {
			err = errors.New("embedding batch length mismatch")
		}
		for i := range vectors {
			if err != nil {
				break
			}
			vectors[i], err = domain.NormalizeVector(vectors[i])
		}
	}
	if err == nil {
		results := make([]IndexCompletion, 0, len(jobs))
		next := 0
		for _, job := range jobs {
			result := IndexCompletion{Job: job}
			if job.Text != "" {
				result.Vector = vectors[next]
				next++
			}
			results = append(results, result)
		}
		if err = ctx.Err(); err == nil {
			err = w.Repository.CompleteBatch(ctx, results)
		}
	}
	if err != nil && ctx.Err() == nil {
		retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		for _, job := range jobs {
			if retryErr := w.Repository.Retry(retryCtx, job); retryErr != nil {
				err = errors.Join(err, retryErr)
			}
		}
	}
	return true, err
}
