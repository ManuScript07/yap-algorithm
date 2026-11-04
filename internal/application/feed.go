package application

import (
	"context"
	"math"
	"time"

	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/ranking"
)

// FeedInput is a bounded, consistent snapshot loaded by the repository.
type FeedInput struct {
	Candidates        []ranking.Candidate
	Interests         []ranking.Interest
	PreferenceVersion int
}

type PreparedFeed struct {
	User, Tab, Model  string
	PreferenceVersion int
	Candidates        []ranking.Candidate
}

type FeedRepository interface {
	LoadFeed(context.Context, string, string) (FeedInput, error)
	// SaveFeed atomically publishes the complete result and checks that the
	// preference version still matches. A reset must invalidate in-flight work.
	SaveFeed(context.Context, PreparedFeed) (string, error)
}

type Scorer interface {
	Score(context.Context, [][]float64) ([]float64, string, error)
}

type FeedBuilder struct {
	Repository FeedRepository
	Scorer     Scorer
	Now        func() time.Time
}

func (b FeedBuilder) Build(ctx context.Context, user, tab string) (string, error) {
	if tab != "for_you" && tab != "friends" && tab != "nearby" {
		return "", domain.ErrInvalid
	}
	input, err := b.Repository.LoadFeed(ctx, user, tab)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	model := "chronological-v1"
	candidates := append([]ranking.Candidate(nil), input.Candidates...)
	if tab == "for_you" {
		model = ranking.Version
		ranking.Score(candidates, input.Interests, now().UTC())
		if b.Scorer != nil && len(candidates) > 0 {
			features := make([][]float64, len(candidates))
			for i := range candidates {
				features[i] = candidates[i].Features
			}
			modelCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			scores, version, scoreErr := b.Scorer.Score(modelCtx, features)
			cancel()
			// Validate the complete response before changing any score. A partial
			// or non-finite response must leave the baseline intact.
			if scoreErr == nil && validScores(scores, version, len(candidates)) {
				for i := range candidates {
					candidates[i].Score = scores[i]
				}
				model = version
			}
		}
		candidates = ranking.Rerank(candidates, 200, 10)
	} else {
		for i := range candidates {
			candidates[i].Features = []float64{}
			candidates[i].Reasons = []string{tab}
			candidates[i].Score = 0
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return b.Repository.SaveFeed(ctx, PreparedFeed{User: user, Tab: tab, Model: model,
		PreferenceVersion: input.PreferenceVersion, Candidates: candidates})
}

func validScores(scores []float64, version string, count int) bool {
	if version == "" || len(scores) != count {
		return false
	}
	for _, score := range scores {
		if math.IsNaN(score) || math.IsInf(score, 0) {
			return false
		}
	}
	return true
}
