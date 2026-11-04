package application

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/ranking"
)

type feedMemory struct {
	input            FeedInput
	saved            *PreparedFeed
	loadErr, saveErr error
	loads            int
}

func (r *feedMemory) LoadFeed(context.Context, string, string) (FeedInput, error) {
	r.loads++
	return r.input, r.loadErr
}
func (r *feedMemory) SaveFeed(_ context.Context, f PreparedFeed) (string, error) {
	r.saved = &f
	return "snapshot", r.saveErr
}

type scoreFunc func(context.Context, [][]float64) ([]float64, string, error)

func (f scoreFunc) Score(c context.Context, v [][]float64) ([]float64, string, error) { return f(c, v) }

func TestFeedBuilderModelFallback(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	var baseline []ranking.Candidate
	for name, scorer := range map[string]Scorer{
		"disabled": nil,
		"outage":   scoreFunc(func(context.Context, [][]float64) ([]float64, string, error) { return nil, "", errors.New("offline") }),
		"partial":  scoreFunc(func(context.Context, [][]float64) ([]float64, string, error) { return []float64{1}, "model", nil }),
		"non_finite": scoreFunc(func(context.Context, [][]float64) ([]float64, string, error) {
			return []float64{100, math.NaN()}, "model", nil
		}),
		"missing_version": scoreFunc(func(context.Context, [][]float64) ([]float64, string, error) { return []float64{1, 2}, "", nil }),
	} {
		t.Run(name, func(t *testing.T) {
			repo := &feedMemory{input: FeedInput{PreferenceVersion: 7, Candidates: []ranking.Candidate{
				{ID: "a", AuthorID: "a", CreatedAt: now}, {ID: "b", AuthorID: "b", CreatedAt: now.Add(-24 * time.Hour)},
			}}}
			id, err := (FeedBuilder{Repository: repo, Scorer: scorer, Now: func() time.Time { return now }}).Build(context.Background(), "viewer", "for_you")
			if err != nil || id != "snapshot" {
				t.Fatalf("build: %s %v", id, err)
			}
			if repo.saved.Model != ranking.Version || repo.saved.PreferenceVersion != 7 {
				t.Fatal("incorrect snapshot versions")
			}
			if baseline == nil {
				baseline = repo.saved.Candidates
			} else if !reflect.DeepEqual(baseline, repo.saved.Candidates) {
				t.Fatal("fallback changed baseline")
			}
		})
	}
}

func TestFeedBuilderPreservesChronology(t *testing.T) {
	for _, tab := range []string{"friends", "nearby"} {
		repo := &feedMemory{input: FeedInput{Candidates: []ranking.Candidate{{ID: "z"}, {ID: "a"}}}}
		scorer := scoreFunc(func(context.Context, [][]float64) ([]float64, string, error) {
			t.Fatal("chronological feed called ranker")
			return nil, "", nil
		})
		_, err := (FeedBuilder{Repository: repo, Scorer: scorer}).Build(context.Background(), "viewer", tab)
		if err != nil || repo.saved.Model != "chronological-v1" || repo.saved.Candidates[0].ID != "z" {
			t.Fatal("chronology changed")
		}
	}
}

func TestFeedBuilderCancellationAndReset(t *testing.T) {
	t.Run("invalid_tab", func(t *testing.T) {
		r := &feedMemory{}
		_, err := (FeedBuilder{Repository: r}).Build(context.Background(), "viewer", "unknown")
		if !errors.Is(err, domain.ErrInvalid) || r.loads != 0 {
			t.Fatal("invalid tab reached repository")
		}
	})
	t.Run("cancel_during_inference", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := &feedMemory{input: FeedInput{Candidates: []ranking.Candidate{{ID: "a"}}}}
		scorer := scoreFunc(func(context.Context, [][]float64) ([]float64, string, error) {
			cancel()
			return []float64{1}, "learned", nil
		})
		_, err := (FeedBuilder{Repository: r, Scorer: scorer}).Build(ctx, "viewer", "for_you")
		if !errors.Is(err, context.Canceled) || r.saved != nil {
			t.Fatal("cancelled result was published")
		}
	})
	t.Run("reset_before_publish", func(t *testing.T) {
		r := &feedMemory{input: FeedInput{PreferenceVersion: 3}, saveErr: domain.ErrInvalidated}
		_, err := (FeedBuilder{Repository: r}).Build(context.Background(), "viewer", "for_you")
		if !errors.Is(err, domain.ErrInvalidated) || r.saved.PreferenceVersion != 3 {
			t.Fatal("reset invalidation was lost")
		}
	})
	t.Run("learned_success", func(t *testing.T) {
		r := &feedMemory{input: FeedInput{Candidates: []ranking.Candidate{{ID: "a"}, {ID: "b"}}}}
		scorer := scoreFunc(func(ctx context.Context, _ [][]float64) ([]float64, string, error) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 250*time.Millisecond {
				t.Fatal("model deadline missing")
			}
			return []float64{0, 10}, "learned-v1", nil
		})
		_, err := (FeedBuilder{Repository: r, Scorer: scorer}).Build(context.Background(), "viewer", "for_you")
		if err != nil || r.saved.Model != "learned-v1" || r.saved.Candidates[0].ID != "b" {
			t.Fatal("learned result not used")
		}
	})
}
