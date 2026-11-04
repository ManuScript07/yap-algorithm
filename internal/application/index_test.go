package application

import (
	"context"
	"errors"
	"math"
	"testing"

	"yap-algorithm/internal/domain"
)

type indexMemory struct {
	job                   IndexJob
	found                 bool
	claimErr, completeErr error
	completed, retries    int
	vector                []float64
}

func (r *indexMemory) Claim(context.Context) (IndexJob, bool, error) {
	return r.job, r.found, r.claimErr
}
func (r *indexMemory) Complete(_ context.Context, _ IndexJob, v []float64) (bool, error) {
	r.completed++
	r.vector = v
	return true, r.completeErr
}
func (r *indexMemory) Retry(context.Context, IndexJob) error { r.retries++; return nil }
func (r *indexMemory) Cleanup(context.Context) error         { return nil }

type encodeFunc func(context.Context, string) ([]float64, error)

func (f encodeFunc) Encode(c context.Context, s string) ([]float64, error) { return f(c, s) }

func TestIndexerRejectsInvalidModelOutput(t *testing.T) {
	for _, kind := range []string{"wrong_dimension", "zero", "nan", "infinity", "outage"} {
		t.Run(kind, func(t *testing.T) {
			r := &indexMemory{job: IndexJob{ID: 1, Text: "text"}, found: true}
			encoder := encodeFunc(func(context.Context, string) ([]float64, error) {
				v := make([]float64, domain.Dimensions)
				switch kind {
				case "wrong_dimension":
					return []float64{1}, nil
				case "nan":
					v[0] = math.NaN()
				case "infinity":
					v[0] = math.Inf(1)
				case "outage":
					return nil, errors.New("offline")
				}
				return v, nil
			})
			worked, err := (Indexer{Repository: r, Encoder: encoder}).Once(context.Background())
			if !worked || err == nil || r.completed != 0 || r.retries != 1 {
				t.Fatal("invalid embedding accepted or retry lost")
			}
		})
	}
}

func TestIndexerSupersededAndSuccessfulJobs(t *testing.T) {
	for _, text := range []string{"", "text"} {
		r := &indexMemory{job: IndexJob{ID: 1, Text: text}, found: true}
		calls := 0
		encoder := encodeFunc(func(context.Context, string) ([]float64, error) {
			calls++
			v := make([]float64, domain.Dimensions)
			v[0] = 3
			v[1] = 4
			return v, nil
		})
		worked, err := (Indexer{Repository: r, Encoder: encoder}).Once(context.Background())
		if !worked || err != nil || r.completed != 1 || r.retries != 0 {
			t.Fatal("job was not completed")
		}
		if text == "" {
			if calls != 0 || r.vector != nil {
				t.Fatal("superseded job called model")
			}
		} else if calls != 1 || math.Abs(r.vector[0]-.6) > 1e-9 {
			t.Fatal("vector not normalized")
		}
	}
}

func TestIndexerDoesNotRetryAfterShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &indexMemory{job: IndexJob{ID: 1, Text: "text"}, found: true}
	encoder := encodeFunc(func(context.Context, string) ([]float64, error) { cancel(); return nil, context.Canceled })
	_, err := (Indexer{Repository: r, Encoder: encoder}).Once(ctx)
	if !errors.Is(err, context.Canceled) || r.retries != 0 || r.completed != 0 {
		t.Fatal("shutdown modified the lease")
	}
}
