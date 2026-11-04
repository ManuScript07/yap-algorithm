package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/ranking"
)

type recommendationMemory struct {
	input   ProfileInput
	result  *RecommendationResult
	err     error
	retries int
	cancel  context.CancelFunc
}

func (r *recommendationMemory) ClaimRefresh(context.Context) (RefreshJob, bool, error) {
	return RefreshJob{UserID: "user", Lease: "lease"}, true, nil
}
func (r *recommendationMemory) LoadProfile(context.Context, RefreshJob) (ProfileInput, error) {
	return r.input, r.err
}
func (r *recommendationMemory) LoadCandidates(context.Context, RefreshJob, ProfileInput, []ranking.Interest) ([]ranking.Candidate, error) {
	if r.cancel != nil {
		r.cancel()
	}
	v := make([]float64, domain.Dimensions)
	v[0] = 1
	return []ranking.Candidate{{ID: "a", AuthorID: "a", PostVersion: 2, CreatedAt: r.input.At, Embedding: v}}, nil
}
func (r *recommendationMemory) Publish(_ context.Context, _ RefreshJob, result RecommendationResult) (bool, error) {
	r.result = &result
	return true, nil
}
func (r *recommendationMemory) RetryRefresh(context.Context, RefreshJob) error {
	r.retries++
	return nil
}
func (r *recommendationMemory) CleanupRecommendations(context.Context) error { return nil }

func TestPersonalizerPublishesVersionedSnapshot(t *testing.T) {
	r := &recommendationMemory{input: ProfileInput{Epoch: 3, Revision: 17, At: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)}}
	worked, err := (Personalizer{Repository: r}).Once(context.Background())
	if err != nil || !worked || r.result == nil {
		t.Fatalf("publish: %v", err)
	}
	if r.result.Input.Epoch != 3 || r.result.Input.Revision != 17 || r.result.FeatureVersion != PersonalizationVersion || r.result.Candidates[0].PostVersion != 2 {
		t.Fatal("snapshot watermarks were lost")
	}
}

func TestPersonalizerFailureAndCancellation(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		r := &recommendationMemory{err: errors.New("db unavailable")}
		_, err := (Personalizer{Repository: r}).Once(context.Background())
		if err == nil || r.retries != 1 || r.result != nil {
			t.Fatal("failed job not retried")
		}
	})
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := &recommendationMemory{input: ProfileInput{At: time.Now()}, cancel: cancel}
		_, err := (Personalizer{Repository: r}).Once(ctx)
		if !errors.Is(err, context.Canceled) || r.retries != 0 || r.result != nil {
			t.Fatal("cancelled job was published/released")
		}
	})
}

type processorFunc func(context.Context) (bool, error)

func (f processorFunc) Once(ctx context.Context) (bool, error) { return f(ctx) }

func TestPipelineFairnessAndEmptyQueue(t *testing.T) {
	counts := [2]int{}
	pipeline := Pipeline{Processors: []Processor{
		processorFunc(func(context.Context) (bool, error) { counts[0]++; return true, nil }),
		processorFunc(func(context.Context) (bool, error) { counts[1]++; return true, nil }),
	}}
	for i := 0; i < 20; i++ {
		if _, err := pipeline.Once(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if counts[0] != 10 || counts[1] != 10 {
		t.Fatal("busy queue starved another queue")
	}
	pipeline = Pipeline{Processors: []Processor{processorFunc(func(context.Context) (bool, error) { return false, nil }), processorFunc(func(context.Context) (bool, error) { return true, nil })}}
	if worked, err := pipeline.Once(context.Background()); err != nil || !worked {
		t.Fatal("empty first queue blocked work")
	}
}
